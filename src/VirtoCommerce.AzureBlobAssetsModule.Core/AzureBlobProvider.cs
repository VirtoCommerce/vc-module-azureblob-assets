using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Options;
using VirtoCommerce.Assets.Abstractions;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.Platform.Core;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Exceptions;
using VirtoCommerce.Platform.Core.Extensions;
using VirtoCommerce.Platform.Core.Settings;
using BlobInfo = VirtoCommerce.AssetsModule.Core.Assets.BlobInfo;

namespace VirtoCommerce.AzureBlobAssetsModule.Core
{
    public class AzureBlobProvider : BasicBlobProvider, IBlobStorageProvider, IBlobUrlResolver, ICommonBlobProvider
    {
        public const string ProviderName = "AzureBlobStorage";
        public const string BlobCacheControlPropertyValue = "public, max-age=604800";
        private const string Delimiter = "/";
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _cdnUrl;
        private readonly string _rootPath;
        private readonly string _containerName;
        private readonly string _relativeRoot;

        public AzureBlobProvider(IOptions<AzureBlobOptions> options, IOptions<PlatformOptions> platformOptions, ISettingsManager settingsManager) : base(platformOptions, settingsManager)
        {
            _blobServiceClient = new BlobServiceClient(options.Value.ConnectionString);
            _cdnUrl = options.Value.CdnUrl;
            _rootPath = options.Value.RootPath;
            _containerName = GetContainerName(options.Value.RootPath);
            _relativeRoot = _rootPath.Substring(_containerName.Length);
        }

        #region ICommonBlobProvider members

        public bool Exists(string blobUrl)
        {
            return ExistsAsync(blobUrl).GetAwaiter().GetResult();
        }

        public async Task<bool> ExistsAsync(string blobUrl)
        {
            var blobInfo = await GetBlobInfoAsync(blobUrl);
            return blobInfo != null;
        }

        #endregion ICommonBlobProvider members

        #region IBlobStorageProvider Members

        /// <summary>
        /// Get blob info by URL
        /// </summary>
        /// <param name="blobUrl">Absolute or relative URL to get blob</param>
        public virtual async Task<BlobInfo> GetBlobInfoAsync(string blobUrl)
        {
            if (string.IsNullOrEmpty(blobUrl))
                throw new ArgumentNullException(nameof(blobUrl));

            //var uri = blobUrl.IsAbsoluteUrl() ? new Uri(blobUrl) : new Uri(_blobServiceClient.Uri, blobUrl.TrimStart(Delimiter[0]));
            BlobInfo result = null;
            try
            {
                var blob = GetBlobClient(blobUrl);
                var props = await blob.GetPropertiesAsync();
                result = ConvertBlobToBlobInfo(blob, props.Value);
            }
            catch
            {
                // Since the storage account is based on transaction volume, it is better to handle the 404 (BlobNotFound) exception because that is just one api call, as opposed to checking the BlobClient.ExistsAsync() first and then making the BlobClient.DownloadAsync() call (2 api transactions).
                //https://elcamino.cloud/articles/2020-03-30-azure-storage-blobs-net-sdk-v12-upgrade-guide-and-tips.html
            }

            return result;
        }

        /// <summary>
        /// Open stream for read blob by relative or absolute url
        /// </summary>
        /// <param name="blobUrl"></param>
        /// <returns>blob stream</returns>
        public virtual Stream OpenRead(string blobUrl)
        {
            return OpenReadAsync(blobUrl).GetAwaiter().GetResult();
        }

        public virtual Task<Stream> OpenReadAsync(string blobUrl)
        {
            if (string.IsNullOrEmpty(blobUrl))
            {
                throw new ArgumentNullException(nameof(blobUrl));
            }

            var blob = GetBlobClient(blobUrl);

            return blob.OpenReadAsync();
        }

        /// <summary>
        /// Open blob for write by relative or absolute url
        /// </summary>
        /// <param name="blobUrl"></param>
        /// <returns>blob stream</returns>
        public virtual Stream OpenWrite(string blobUrl)
        {
            return OpenWriteAsync(blobUrl).GetAwaiter().GetResult();
        }

        public virtual async Task<Stream> OpenWriteAsync(string blobUrl)
        {
            if (string.IsNullOrEmpty(blobUrl))
            {
                throw new ArgumentNullException(nameof(blobUrl));
            }

            var fileName = Path.GetFileName(blobUrl);

            if (IsExtensionBlacklisted(blobUrl))
            {
                throw new PlatformException($"This extension is not allowed. Please contact administrator.");
            }

            var container = GetBlobContainerClient();

            var blob = container.GetBlockBlobClient(UrlHelperExtensions.Combine(_relativeRoot, blobUrl));

            var options = new BlockBlobOpenWriteOptions
            {
                HttpHeaders = new BlobHttpHeaders
                {
                    // Use HTTP response headers to instruct the browser regarding the safe use of uploaded files, when downloaded from the system
                    ContentType = MimeTypeResolver.ResolveContentType(fileName),
                    // Leverage Browser Caching - 7days
                    // Setting Cache-Control on Azure Blobs can help reduce bandwidth and improve the performance by preventing consumers from having to continuously download resources.
                    // More Info https://developers.google.com/speed/docs/insights/LeverageBrowserCaching
                    CacheControl = BlobCacheControlPropertyValue
                }
            };

            // FlushLessStream wraps BlockBlobWriteStream to not use Flush multiple times.
            // !!! Call Flush several times on a plain BlockBlobWriteStream causes stream hangs/errors.
            // https://github.com/Azure/azure-sdk-for-net/issues/20652
            return new FlushLessStream(await blob.OpenWriteAsync(true, options));
        }

        public virtual async Task RemoveAsync(string[] urls)
        {
            foreach (var url in urls.Where(x => !string.IsNullOrWhiteSpace(x)))
            {
                var blobContainer = GetBlobContainerClient();

                var prefix = UrlHelperExtensions.Combine(_relativeRoot, url).TrimStart('/');
                var blobItems = blobContainer.GetBlobs(prefix: prefix);
                foreach (var blobItem in blobItems)
                {
                    var blobClient = blobContainer.GetBlobClient(blobItem.Name);
                    await blobClient.DeleteIfExistsAsync();
                }
            }
        }

        public virtual async Task<BlobEntrySearchResult> SearchAsync(string folderUrl, string keyword)
        {
            var result = AbstractTypeFactory<BlobEntrySearchResult>.TryCreateInstance();

            folderUrl ??= "";

            var container = GetBlobContainerClient();

            if (container != null)
            {
                var baseUriEscaped = container.Uri.AbsoluteUri;
                var prefix = UrlHelperExtensions.Combine(_relativeRoot, folderUrl).TrimStart('/');
                if (!string.IsNullOrEmpty(keyword))
                {
                    //Only whole container list allow search by prefix
                    prefix = UrlHelperExtensions.Combine(prefix, keyword);
                }

                var containerProperties = await container.GetPropertiesAsync();

                // Call the listing operation and return pages of the specified size.
                var resultSegment = container.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: Delimiter)
                    .AsPages();

                // Enumerate the blobs returned for each page.
                await foreach (var blobPage in resultSegment)
                {
                    // A hierarchical listing may return both virtual directories and blobs.
                    foreach (var blobHierarchyItem in blobPage.Values)
                    {
                        if (blobHierarchyItem.IsPrefix)
                        {
                            var folder = AbstractTypeFactory<BlobFolder>.TryCreateInstance();

                            // No Unescaped for Name. Do a string that has been previously unescaped can lead to ambiguities and errors.
                            folder.Name = blobHierarchyItem.Prefix
                               .Split(new[] { Delimiter }, StringSplitOptions.RemoveEmptyEntries)
                               .Last();

                            var folderUrlBuilder = new UriBuilder(new Uri(baseUriEscaped));
                            folderUrlBuilder.Path += Delimiter + blobHierarchyItem.Prefix;
                            folder.Url = folderUrlBuilder.Uri.AbsoluteUri;
                            folder.ParentUrl = GetParentUrl(baseUriEscaped, blobHierarchyItem.Prefix);
                            folder.RelativeUrl = Uri.UnescapeDataString(folderUrlBuilder.Path);

                            var prefixPath = "/" + _rootPath;
                            if (folder.RelativeUrl.StartsWith(prefixPath))
                            {
                                folder.RelativeUrl = $"/{folder.RelativeUrl[prefixPath.Length..]}";
                            }


                            folder.CreatedDate = containerProperties.Value.LastModified.UtcDateTime;
                            folder.ModifiedDate = containerProperties.Value.LastModified.UtcDateTime;

                            result.Results.Add(folder);
                        }
                        else
                        {
                            var blobInfo = ConvertBlobToBlobInfo(blobHierarchyItem.Blob, baseUriEscaped);
                            //Do not return empty blob (created with directory because azure blob not support direct directory creation)
                            if (!string.IsNullOrEmpty(blobInfo.Name))
                            {
                                result.Results.Add(blobInfo);
                            }
                        }
                    }
                }
            }

            result.TotalCount = result.Results.Count;
            return result;
        }

        public virtual async Task CreateFolderAsync(BlobFolder folder)
        {
            var newFolderUrl = folder.Name;

            if (folder.ParentUrl != null)
            {
                var newFolderUriBuilder = new UriBuilder(new Uri(folder.ParentUrl));
                newFolderUriBuilder.Path += Delimiter + folder.Name;
                newFolderUrl = newFolderUriBuilder.ToString();
            }

            var container = GetBlobContainerClient();
            await container.CreateIfNotExistsAsync(PublicAccessType.Blob);

            var directoryPath = GetDirectoryPathFromUrl(newFolderUrl);
            if (!string.IsNullOrEmpty(directoryPath))
            {
                // Need to upload an empty '.keep' blob because Azure Blob Storage does not support direct directory creation
                using var stream = new MemoryStream(new byte[0]);
                await container.GetBlockBlobClient($"{directoryPath}.keep").UploadAsync(stream);
            }
        }

        public virtual void Move(string srcUrl, string destUrl)
        {
            MoveAsync(srcUrl, destUrl).GetAwaiter().GetResult();
        }

        public virtual Task MoveAsyncPublic(string srcUrl, string destUrl)
        {
            return MoveAsync(srcUrl, destUrl);
        }

        public virtual void Copy(string srcUrl, string destUrl)
        {
            MoveAsync(srcUrl, destUrl, true).GetAwaiter().GetResult();
        }

        public virtual Task CopyAsync(string srcUrl, string destUrl)
        {
            return MoveAsync(srcUrl, destUrl, true);
        }

        protected virtual async Task MoveAsync(string oldUrl, string newUrl, bool isCopy = false)
        {
            string oldPath, newPath;
            var isFolderRename = string.IsNullOrEmpty(Path.GetFileName(oldUrl));

            //if rename file
            if (!isFolderRename)
            {
                oldPath = GetFilePathFromUrl(oldUrl);
                newPath = GetFilePathFromUrl(newUrl);
            }
            else
            {
                oldPath = GetDirectoryPathFromUrl(oldUrl);
                newPath = GetDirectoryPathFromUrl(newUrl);
            }

            var taskList = new List<Task>();
            var blobContainer = GetBlobContainerClient();
            var blobItems = blobContainer.GetBlobsAsync(prefix: oldPath);

            await foreach (var blobItem in blobItems)
            {
                var blobName = blobItem.Name;
                var newBlobName = blobName.Replace(oldPath, newPath);

                taskList.Add(MoveBlob(blobContainer, blobName, newBlobName, isCopy));
            }

            await Task.WhenAll(taskList);
        }

        /// <summary>
        /// Move blob new URL and remove old blob
        /// </summary>
        /// <param name="container"></param>
        /// <param name="oldUrl"></param>
        /// <param name="newUrl"></param>
        /// <param name="isCopy"></param>
        private async Task MoveBlob(BlobContainerClient container, string oldUrl, string newUrl, bool isCopy)
        {
            var targetPath = newUrl.EndsWith(Delimiter)
                ? GetDirectoryPathFromUrl(newUrl)
                : GetFilePathFromUrl(newUrl);

            if (IsExtensionBlacklisted(targetPath))
            {
                throw new PlatformException($"This extension is not allowed. Please contact administrator.");
            }

            var target = container.GetBlockBlobClient(targetPath);

            if (!await target.ExistsAsync())
            {
                var sourcePath = oldUrl.EndsWith(Delimiter)
                    ? GetDirectoryPathFromUrl(oldUrl)
                    : GetFilePathFromUrl(oldUrl);

                var sourceBlob = container.GetBlockBlobClient(sourcePath);

                if (await sourceBlob.ExistsAsync())
                {
                    await target.StartCopyFromUriAsync(sourceBlob.Uri);

                    if (!isCopy)
                    {
                        await sourceBlob.DeleteIfExistsAsync();
                    }
                }
            }
        }

        #endregion IBlobStorageProvider Members

        #region IBlobUrlResolver Members

        public virtual string GetAbsoluteUrl(string blobKey)
        {
            var result = blobKey;
            if (!blobKey.IsAbsoluteUrl())
            {
                var baseUrl = _blobServiceClient.Uri.AbsoluteUri;

                if (!string.IsNullOrWhiteSpace(_cdnUrl))
                {
                    var cdnUriBuilder = new UriBuilder(_blobServiceClient.Uri.Scheme, _cdnUrl);
                    baseUrl = cdnUriBuilder.Uri.AbsoluteUri;
                }

                baseUrl = UrlHelperExtensions.Combine(baseUrl, _rootPath);

                result = UrlHelperExtensions.Combine(baseUrl, EscapeUri(blobKey));
            }

            return result;
        }

        #endregion IBlobUrlResolver Members

        /// <summary>
        /// Return outline folder from absolute or relative URL
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        private string[] GetOutlineFromUrl(string url)
        {
            var relativeUrl = url;

            if (url.IsAbsoluteUrl())
            {
                relativeUrl = Uri.UnescapeDataString(new Uri(url).AbsolutePath);
            }

            return relativeUrl.Split(new[] { Delimiter, "\\" },
                StringSplitOptions.RemoveEmptyEntries);
        }

        private string GetContainerName(string path)
        {
            return GetOutlineFromUrl(path).First();
        }

        private string GetDirectoryPathFromUrl(string url)
        {
            var result = string.Join(Delimiter, GetOutlineFromUrl(url).Skip(1).ToArray());
            return !string.IsNullOrEmpty(result) ? Uri.UnescapeDataString(result) + Delimiter : null;
        }

        private string GetFilePathFromUrl(string url)
        {
            var result = string.Join(Delimiter, GetOutlineFromUrl(url).Skip(1).ToArray());
            return !string.IsNullOrEmpty(result) ? Uri.UnescapeDataString(result) : null;
        }

        private string GetParentUrl(string baseUri, string blobPrefix)
        {
            var segments = GetOutlineFromUrl(blobPrefix);
            var parentPath = string.Join(Delimiter, segments.Take(segments.Length - 1));
            return UrlHelperExtensions.Combine(baseUri, EscapeUri(parentPath));
        }

        private static string EscapeUri(string stringToEscape)
        {
            var parts = stringToEscape.Split(Delimiter);
            parts = parts.Select(Uri.EscapeDataString).ToArray();
            var result = string.Join(Delimiter, parts);
            return result;



            //var fileName = Path.GetFileName(stringToEscape);
            //var blobPath = string.IsNullOrEmpty(fileName) ? stringToEscape : stringToEscape.Replace(fileName, string.Empty);
            //var escapedFileName = Uri.EscapeDataString(fileName);

            //return $"{blobPath}{escapedFileName}";
        }

        private BlobContainerClient GetBlobContainerClient()
        {
            BlobContainerClient result = null;

            // Retrieve container reference.
            var container = _blobServiceClient.GetBlobContainerClient(_containerName);
            if (container.Exists())
            {
                result = container;
            }

            return result;
        }

        private BlobClient GetBlobClient(string relativeUrl)
        {
            var container = GetBlobContainerClient();
            return container.GetBlobClient(UrlHelperExtensions.Combine(_relativeRoot, relativeUrl));
        }

        private BlobInfo ConvertBlobToBlobInfo(BlobClient blob, BlobProperties props)
        {
            var absoluteUrl = blob.Uri.AbsoluteUri;
            var relativeUrl = blob.Uri.LocalPath;

            var prefix = "/" + _rootPath;
            if (relativeUrl.StartsWith(prefix))
            {
                relativeUrl = "/" + relativeUrl.Substring(prefix.Length);
            }

            var fileName = Path.GetFileName(Uri.UnescapeDataString(blob.Name));
            var contentType = MimeTypeResolver.ResolveContentType(fileName);

            return new BlobInfo
            {
                Url = absoluteUrl,
                Name = fileName,
                ContentType = contentType,
                Size = props.ContentLength,
                CreatedDate = props.CreatedOn.UtcDateTime,
                ModifiedDate = props.LastModified.UtcDateTime,
                RelativeUrl = relativeUrl
            };
        }

        private BlobInfo ConvertBlobToBlobInfo(BlobItem blob, string baseUri)
        {
            var fileUrlBuilder = new UriBuilder(new Uri(baseUri));
            fileUrlBuilder.Path = fileUrlBuilder.Path + "/" + blob.Name;
            var absoluteUri = fileUrlBuilder.Uri;
            var absoluteUrl = absoluteUri.AbsoluteUri;

            var relativeUrl = absoluteUri.LocalPath;
            var prefix = "/" + _rootPath;
            if (relativeUrl.StartsWith(prefix))
            {
                relativeUrl = string.Concat("/", relativeUrl.AsSpan(prefix.Length));
            }

            var fileName = Path.GetFileName(blob.Name);
            var contentType = MimeTypeResolver.ResolveContentType(fileName);

            return new BlobInfo
            {
                Url = absoluteUrl,
                Name = fileName,
                ContentType = contentType,
                Size = blob.Properties.ContentLength ?? 0,
                CreatedDate = blob.Properties.CreatedOn?.UtcDateTime ?? DateTime.MinValue,
                ModifiedDate = blob.Properties.LastModified?.UtcDateTime,
                RelativeUrl = relativeUrl
            };
        }
    }
}
