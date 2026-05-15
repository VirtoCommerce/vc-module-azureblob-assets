using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using VirtoCommerce.Assets.Abstractions;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.AssetsModule.Core.Events;
using VirtoCommerce.AssetsModule.Core.Model;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Events;
using VirtoCommerce.Platform.Core.Exceptions;
using VirtoCommerce.Platform.Core.Extensions;
using BlobInfo = VirtoCommerce.AssetsModule.Core.Assets.BlobInfo;

namespace VirtoCommerce.AzureBlobAssetsModule.Core
{
    public class AzureBlobProvider : IBlobStorageProvider, IBlobUrlResolver, ICommonBlobProvider, IAzureBlobUrlResolver
    {
        public const string ProviderName = "AzureBlobStorage";
        public const string BlobCacheControlPropertyValue = "public, max-age=604800";
        private const string Delimiter = "/";
        private readonly BlobServiceClient _blobServiceClient;
        private readonly Uri _publicBaseUri;
        private readonly bool _allowBlobPublicAccess;
        private readonly IFileExtensionService _fileExtensionService;
        private readonly IEventPublisher _eventPublisher;
        private readonly ILogger<AzureBlobProvider> _logger;

        public AzureBlobProvider(
            IOptions<AzureBlobOptions> options,
            IFileExtensionService fileExtensionService,
            IEventPublisher eventPublisher)
            : this(options, fileExtensionService, eventPublisher, NullLogger<AzureBlobProvider>.Instance)
        {
        }

        public AzureBlobProvider(
            IOptions<AzureBlobOptions> options,
            IFileExtensionService fileExtensionService,
            IEventPublisher eventPublisher,
            ILogger<AzureBlobProvider> logger)
        {
            _blobServiceClient = new BlobServiceClient(options.Value.ConnectionString);
            _logger = logger ?? NullLogger<AzureBlobProvider>.Instance;
            _publicBaseUri = NormalizePublicUrl(options.Value.PublicUrl, _blobServiceClient.Uri.Scheme);
            _allowBlobPublicAccess = options.Value.AllowBlobPublicAccess;
            _fileExtensionService = fileExtensionService;
            _eventPublisher = eventPublisher;

            _logger.LogInformation(
                "AzureBlobProvider initialized. BlobServiceUri='{BlobServiceUri}', PublicUrlConfigured='{PublicUrlValue}', PublicBaseUri='{PublicBaseUri}', AllowBlobPublicAccess={AllowBlobPublicAccess}",
                _blobServiceClient.Uri,
                string.IsNullOrWhiteSpace(options.Value.PublicUrl) ? "(empty)" : options.Value.PublicUrl,
                _publicBaseUri?.ToString() ?? "(null - falls back to blob service Uri)",
                _allowBlobPublicAccess);
        }

        private static Uri NormalizePublicUrl(string publicUrl, string defaultScheme)
        {
            if (string.IsNullOrWhiteSpace(publicUrl))
            {
                return null;
            }

            if (publicUrl.Contains("://", StringComparison.Ordinal)
                && Uri.TryCreate(publicUrl, UriKind.Absolute, out var absolute))
            {
                return absolute;
            }

            return new UriBuilder(defaultScheme, publicUrl).Uri;
        }

        private Uri GetPublicBaseUri() => _publicBaseUri ?? _blobServiceClient.Uri;

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
            {
                throw new ArgumentNullException(nameof(blobUrl));
            }

            BlobInfo result = null;
            try
            {
                var blob = GetBlockBlobClient(blobUrl);
                var props = await blob.GetPropertiesAsync();
                var blobTagResult = await blob.GetTagsAsync();
                result = ConvertToBlobInfo(blob, props.Value, blobTagResult.Value?.Tags);
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

            var blob = GetBlockBlobClient(blobUrl);

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
            var filePath = GetFilePathFromUrl(blobUrl);

            if (filePath == null)
            {
                throw new ArgumentException("Cannot get file path from URL", nameof(blobUrl));
            }

            if (!await _fileExtensionService.IsExtensionAllowedAsync(filePath))
            {
                throw new PlatformException($"File extension {Path.GetExtension(filePath)} is not allowed. Please contact administrator.");
            }

            var container = await CreateContainerIfNotExists(blobUrl);
            var blob = container.GetBlockBlobClient(filePath);

            var options = new BlockBlobOpenWriteOptions
            {
                HttpHeaders = new BlobHttpHeaders
                {
                    // Use HTTP response headers to instruct the browser regarding the safe use of uploaded files, when downloaded from the system
                    ContentType = MimeTypeResolver.ResolveContentType(filePath),
                    // Leverage Browser Caching - 7days
                    // Setting Cache-Control on Azure Blobs can help reduce bandwidth and improve the performance by preventing consumers from having to continuously download resources.
                    // More Info https://developers.google.com/speed/docs/insights/LeverageBrowserCaching
                    CacheControl = BlobCacheControlPropertyValue
                }
            };

            // FlushLessStream wraps BlockBlobWriteStream to not use Flush multiple times.
            // !!! Call Flush several times on a plain BlockBlobWriteStream causes stream hangs/errors.
            // https://github.com/Azure/azure-sdk-for-net/issues/20652
            return new BlobUploadStream(new FlushLessStream(await blob.OpenWriteAsync(true, options)), blobUrl, ProviderName, _eventPublisher);
        }

        public virtual async Task RemoveAsync(string[] urls)
        {
            ArgumentNullException.ThrowIfNull(urls);

            var urlsToDelete = urls.Where(x => !string.IsNullOrWhiteSpace(x)).ToArray();

            if (urlsToDelete.Length == 0)
            {
                return;
            }

            foreach (var url in urlsToDelete)
            {
                var uri = GetAbsoluteUri(_blobServiceClient.Uri, url);
                var absoluteUri = uri.AbsoluteUri;
                var container = GetBlobContainerClient(absoluteUri);

                var isFolder = string.IsNullOrEmpty(Path.GetFileName(absoluteUri));
                var blobSearchPrefix = isFolder
                    ? GetDirectoryPathFromUrl(absoluteUri)
                    : GetFilePathFromUrl(absoluteUri);

                if (string.IsNullOrEmpty(blobSearchPrefix))
                {
                    await container.DeleteIfExistsAsync();
                }
                else
                {
                    if (isFolder)
                    {
                        // Delete all blobs with the prefix (folder)
                        var blobItems = container.GetBlobsAsync(BlobTraits.None, BlobStates.None, blobSearchPrefix, CancellationToken.None);
                        await foreach (var blobItem in blobItems)
                        {
                            var blobClient = container.GetBlobClient(blobItem.Name);
                            await blobClient.DeleteIfExistsAsync();
                        }
                    }
                    else
                    {
                        // Delete only the exact file
                        var blobClient = container.GetBlobClient(blobSearchPrefix);
                        await blobClient.DeleteIfExistsAsync();
                    }
                }
            }

            await RaiseBlobDeletedEvent(urlsToDelete);
        }

        protected virtual Task RaiseBlobDeletedEvent(string[] urls)
        {
            if (_eventPublisher != null)
            {
                var events = urls.Select(url =>
                new GenericChangedEntry<BlobEventInfo>(new BlobEventInfo
                {
                    Id = url,
                    Uri = url,
                    Provider = ProviderName
                }, EntryState.Deleted)).ToArray();

                return _eventPublisher.Publish(new BlobDeletedEvent(events));
            }

            return Task.CompletedTask;
        }

        public virtual async Task<BlobEntrySearchResult> SearchAsync(string folderUrl, string keyword)
        {
            var result = AbstractTypeFactory<BlobEntrySearchResult>.TryCreateInstance();

            if (!string.IsNullOrEmpty(folderUrl))
            {
                var container = GetBlobContainerClient(folderUrl);

                if (await container.ExistsAsync())
                {
                    var internalBaseUri = container.Uri; // absoluteUri is escaped already
                    var publicBaseUri = GetAbsoluteUri(GetPublicBaseUri(), container.Name + Delimiter);
                    _logger.LogDebug(
                        "SearchAsync: folderUrl='{FolderUrl}', container='{Container}', internalBaseUri='{InternalBaseUri}', publicBaseUri='{PublicBaseUri}', publicUrlConfigured={PublicUrlConfigured}",
                        folderUrl,
                        container.Name,
                        internalBaseUri,
                        publicBaseUri,
                        _publicBaseUri != null);
                    var prefix = GetDirectoryPathFromUrl(folderUrl);
                    if (!string.IsNullOrEmpty(keyword))
                    {
                        //Only whole container list allow search by prefix
                        prefix += keyword;
                    }

                    var containerProperties = await container.GetPropertiesAsync();

                    // Call the listing operation and return pages of the specified size.
                    var resultSegment = container.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, Delimiter, prefix, CancellationToken.None).AsPages();

                    // Enumerate the blobs returned for each page.
                    await foreach (var blobPage in resultSegment)
                    {
                        // A hierarchical listing may return both virtual directories and blobs.
                        foreach (var blobHierarchyItem in blobPage.Values)
                        {
                            if (blobHierarchyItem.IsPrefix)
                            {
                                var folder = ConvertToBlobFolder(blobHierarchyItem, publicBaseUri, internalBaseUri, containerProperties.Value);
                                result.Results.Add(folder);
                            }
                            else
                            {
                                var blobInfo = ConvertToBlobInfo(blobHierarchyItem.Blob, publicBaseUri, internalBaseUri);
                                //Do not return empty blob (created with directory because azure blob not support direct directory creation)
                                if (!string.IsNullOrEmpty(blobInfo.Name))
                                {
                                    result.Results.Add(blobInfo);
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                // Call the listing operation and enumerate the result segment.
                var resultSegment = _blobServiceClient.GetBlobContainersAsync(prefix: keyword).AsPages();

                await foreach (var containerPage in resultSegment)
                {
                    var folders = containerPage.Values.Select(ConvertToBlobFolder);
                    result.Results.AddRange(folders);
                }
            }

            result.TotalCount = result.Results.Count;
            return result;
        }

        public virtual async Task CreateFolderAsync(BlobFolder folder)
        {
            var path = folder.Name;
            if (!string.IsNullOrEmpty(folder.ParentUrl))
            {
                var parentUrl = folder.ParentUrl.IsAbsoluteUrl()
                    ? GetRelativeUrl(new Uri(folder.ParentUrl))
                    : folder.ParentUrl;
                path = UrlHelperExtensions.Combine(parentUrl, folder.Name);
            }

            var container = await CreateContainerIfNotExists(path);

            var directoryPath = GetDirectoryPathFromUrl(path);
            if (!string.IsNullOrEmpty(directoryPath))
            {
                //Need upload empty blob because azure blob storage not support direct directory creation
                using var stream = new MemoryStream(Array.Empty<byte>());
                var keepFile = string.Join(Delimiter, directoryPath.TrimEnd(Delimiter[0]), ".keep");
                await container.GetBlockBlobClient(keepFile).UploadAsync(stream);
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
            string oldPath;
            string newPath;
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
            var container = GetBlobContainerClient(oldUrl);
            var blobItems = container.GetBlobsAsync(BlobTraits.None, BlobStates.None, oldPath, CancellationToken.None);

            await foreach (var blobItem in blobItems)
            {
                var blobName = UrlHelperExtensions.Combine(container.Name, blobItem.Name);
                var newBlobName = blobName.Replace(oldPath, newPath);

                taskList.Add(MoveBlob(container, blobName, newBlobName, isCopy));
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

            if (!await _fileExtensionService.IsExtensionAllowedAsync(targetPath))
            {
                throw new PlatformException($"File extension {Path.GetExtension(targetPath)} is not allowed. Please contact administrator.");
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
                    await (await target.StartCopyFromUriAsync(sourceBlob.Uri)).WaitForCompletionAsync();

                    if (!isCopy)
                    {
                        await sourceBlob.DeleteIfExistsAsync();
                    }
                }
            }
        }

        #endregion IBlobStorageProvider Members

        #region IBlobUrlResolver Members

        public string GetAbsoluteUrl(string inputUrl)
        {
            ArgumentNullException.ThrowIfNull(inputUrl);

            return GetAbsoluteUri(GetPublicBaseUri(), inputUrl).AbsoluteUri;
        }

        #endregion IBlobUrlResolver Members

        #region IAzureBlobUrlResolver Members

        public string GenerateSasUrl(string blobUrl, TimeSpan expiresIn)
        {
            var blobName = GetFilePathFromUrl(blobUrl);
            var containerName = GetContainerNameFromUrl(blobUrl);
            var blobClient = GetBlobContainerClient(blobUrl)
                .GetBlobClient(blobName);

            var sasBuilder = new BlobSasBuilder
            {
                BlobContainerName = containerName,
                BlobName = blobName,
                Resource = "b",
                ExpiresOn = DateTimeOffset.UtcNow.Add(expiresIn)
            };

            sasBuilder.SetPermissions(BlobSasPermissions.Read);

            return blobClient.GenerateSasUri(sasBuilder).ToString();
        }

        #endregion

        protected virtual BlobInfo ConvertToBlobInfo(BlobBaseClient blob, BlobProperties props, IDictionary<string, string> tags)
        {
            var blobInfo = AbstractTypeFactory<BlobInfo>.TryCreateInstance();

            var relativeUrl = GetRelativeUrl(blob.Uri);
            var publicUri = GetAbsoluteUri(GetPublicBaseUri(), relativeUrl);
            var fileName = Path.GetFileName(Uri.UnescapeDataString(blob.Name));

            blobInfo.Url = publicUri.AbsoluteUri;
            blobInfo.RelativeUrl = relativeUrl;
            blobInfo.Name = fileName;
            blobInfo.ContentType = props.ContentType.EmptyToNull() ?? MimeTypeResolver.ResolveContentType(fileName);
            blobInfo.Size = props.ContentLength;
            blobInfo.CreatedDate = props.CreatedOn.UtcDateTime;
            blobInfo.ModifiedDate = props.LastModified.UtcDateTime;

            return blobInfo;
        }

        [Obsolete("Use ConvertToBlobInfo(BlobItem, Uri publicBaseUri, Uri internalBaseUri) overload.")]
        protected virtual BlobInfo ConvertToBlobInfo(BlobItem blob, Uri baseUri)
        {
            return ConvertToBlobInfo(blob, baseUri, baseUri);
        }

        protected virtual BlobInfo ConvertToBlobInfo(BlobItem blob, Uri publicBaseUri, Uri internalBaseUri)
        {
            var blobInfo = AbstractTypeFactory<BlobInfo>.TryCreateInstance();

            var publicAbsoluteUri = GetAbsoluteUri(publicBaseUri, blob.Name);
            var internalAbsoluteUri = GetAbsoluteUri(internalBaseUri, blob.Name);
            var fileName = Path.GetFileName(Uri.UnescapeDataString(blob.Name));

            blobInfo.Url = publicAbsoluteUri.AbsoluteUri;
            blobInfo.RelativeUrl = GetRelativeUrl(internalAbsoluteUri);
            blobInfo.Name = fileName;
            blobInfo.ContentType = blob.Properties.ContentType.EmptyToNull() ?? MimeTypeResolver.ResolveContentType(fileName);
            blobInfo.Size = blob.Properties.ContentLength ?? 0;
            blobInfo.CreatedDate = blob.Properties.CreatedOn!.Value.UtcDateTime;
            blobInfo.ModifiedDate = blob.Properties.LastModified?.UtcDateTime;

            return blobInfo;
        }

        [Obsolete("Use ConvertToBlobFolder(BlobHierarchyItem, Uri publicBaseUri, Uri internalBaseUri, BlobContainerProperties) overload.")]
        protected virtual BlobFolder ConvertToBlobFolder(BlobHierarchyItem blobHierarchyItem, Uri baseUri, BlobContainerProperties containerProperties)
        {
            return ConvertToBlobFolder(blobHierarchyItem, baseUri, baseUri, containerProperties);
        }

        protected virtual BlobFolder ConvertToBlobFolder(
            BlobHierarchyItem blobHierarchyItem,
            Uri publicBaseUri,
            Uri internalBaseUri,
            BlobContainerProperties containerProperties)
        {
            var folder = AbstractTypeFactory<BlobFolder>.TryCreateInstance();

            var publicAbsoluteUri = GetAbsoluteUri(publicBaseUri, blobHierarchyItem.Prefix);
            var internalAbsoluteUri = GetAbsoluteUri(internalBaseUri, blobHierarchyItem.Prefix);

            folder.Name = GetOutlineFromUrl(blobHierarchyItem.Prefix).Last();
            folder.Url = publicAbsoluteUri.AbsoluteUri;
            folder.ParentUrl = GetParentUrl(publicBaseUri, blobHierarchyItem.Prefix);
            folder.RelativeUrl = GetRelativeUrl(internalAbsoluteUri);
            folder.ModifiedDate = folder.CreatedDate = containerProperties.LastModified.UtcDateTime;

            return folder;
        }

        protected virtual BlobFolder ConvertToBlobFolder(BlobContainerItem container)
        {
            var folder = AbstractTypeFactory<BlobFolder>.TryCreateInstance();

            var publicAbsoluteUri = GetAbsoluteUri(GetPublicBaseUri(), container.Name);
            var internalAbsoluteUri = GetAbsoluteUri(_blobServiceClient.Uri, container.Name);

            folder.Name = container.Name.Split(Delimiter).Last();
            folder.Url = publicAbsoluteUri.AbsoluteUri;
            folder.RelativeUrl = GetRelativeUrl(internalAbsoluteUri);
            folder.ModifiedDate = folder.CreatedDate = container.Properties.LastModified.UtcDateTime;

            return folder;
        }

        protected async Task<BlobContainerClient> CreateContainerIfNotExists(string blobUrl)
        {
            var container = GetBlobContainerClient(blobUrl);

            if (!await container.ExistsAsync())
            {
                var accessType = _allowBlobPublicAccess ? PublicAccessType.Blob : PublicAccessType.None;
                await container.CreateAsync(accessType);
            }

            return container;
        }

        protected BlockBlobClient GetBlockBlobClient(string blobUrl)
        {
            var filePath = GetFilePathFromUrl(blobUrl);
            var container = GetBlobContainerClient(blobUrl);
            var blob = container.GetBlockBlobClient(filePath);

            return blob;
        }

        protected BlobContainerClient GetBlobContainerClient(string blobUrl)
        {
            var containerName = GetContainerNameFromUrl(blobUrl);
            var container = _blobServiceClient.GetBlobContainerClient(containerName);

            return container;
        }

        /// <summary>
        /// Return outline folder from absolute or relative URL
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        private static string[] GetOutlineFromUrl(string url)
        {
            var relativeUrl = url;
            if (url.IsAbsoluteUrl())
            {
                relativeUrl = Uri.UnescapeDataString(new Uri(url).AbsolutePath);
            }

            var start = 0;
            var end = 0;
            if (relativeUrl.StartsWith(Delimiter))
            {
                start++;
            }
            if (relativeUrl.EndsWith(Delimiter))
            {
                end++;
            }
            relativeUrl = relativeUrl[start..^end];

            return relativeUrl.Split(Delimiter[0], '\\'); // name may be empty
        }

        private static string GetContainerNameFromUrl(string url)
        {
            return GetOutlineFromUrl(url).First();
        }

        private static string GetDirectoryPathFromUrl(string url)
        {
            var result = string.Join(Delimiter, GetOutlineFromUrl(url).Skip(1).ToArray());
            return !string.IsNullOrEmpty(result) ? HttpUtility.UrlDecode(result) + Delimiter : null;
        }

        private static string GetFilePathFromUrl(string url)
        {
            var result = string.Join(Delimiter, GetOutlineFromUrl(url).Skip(1).ToArray());
            return !string.IsNullOrEmpty(result) ? HttpUtility.UrlDecode(result) : null;
        }

        private static string GetParentUrl(Uri baseUri, string blobPrefix)
        {
            var baseUriString = baseUri.GetLeftPart(UriPartial.Path);
            var baseUriQuery = baseUri.Query;
            var result = GetParentUrl(baseUriString, blobPrefix, baseUriQuery);

            return result;
        }

        private static string GetParentUrl(string baseUri, string blobPrefix, string query = null)
        {
            var segments = GetOutlineFromUrl(blobPrefix);
            var parentPath = string.Join(Delimiter, segments.Take(segments.Length - 1));
            var result = GetAbsoluteUri(baseUri, parentPath, query).AbsoluteUri;

            return result;
        }

        private string GetRelativeUrl(Uri absoluteUri)
        {
            if (!string.IsNullOrEmpty(absoluteUri.Query))
            {
                absoluteUri = new Uri(absoluteUri.GetLeftPart(UriPartial.Path));
            }
            var result = _blobServiceClient.Uri.MakeRelativeUri(absoluteUri).ToString();

            // Ensure the relative URL starts with a delimiter
            if (!result.StartsWith(Delimiter))
            {
                result = Delimiter + result;
            }

            return result;
        }

        public static Uri GetAbsoluteUri(Uri baseUri, string inputUrl)
        {
            ArgumentNullException.ThrowIfNull(baseUri);
            ArgumentException.ThrowIfNullOrEmpty(inputUrl);

            var baseUriString = baseUri.GetLeftPart(UriPartial.Path);
            var baseUriQuery = baseUri.Query;
            var result = GetAbsoluteUri(baseUriString, inputUrl, baseUriQuery);

            return result;
        }

        private static Uri GetAbsoluteUri(string baseUri, string inputUrl, string query)
        {
            var result = GetAbsoluteUri(baseUri, inputUrl);

            if (!string.IsNullOrEmpty(query))
            {
                result = new UriBuilder(result) { Query = query }.Uri;
            }

            return result;
        }

        private static Uri GetAbsoluteUri(string baseUri, string inputUrl)
        {
            // base uri should be ended with delimiter. see tests
            if (!baseUri.EndsWith(Delimiter))
            {
                baseUri += Delimiter;
            }

            // do trim lead slash to prevent transform it to absolute file path on linux.
            if (Uri.TryCreate(inputUrl.TrimStart('/'), UriKind.Absolute, out var resultUri))
            {
                // If the input URL is already absolute, return it as is (with correct encoding)
                return resultUri;
            }

            if (inputUrl.StartsWith('/'))
            {
                inputUrl = "." + inputUrl;
            }
            else if (!inputUrl.StartsWith('.'))
            {
                inputUrl = "./" + inputUrl;
            }

            // If the input URL is relative, combine it with the base URI
            if (Uri.TryCreate(new Uri(baseUri), inputUrl, out resultUri))
            {
                return resultUri;
            }

            return new Uri(inputUrl);
        }
    }
}
