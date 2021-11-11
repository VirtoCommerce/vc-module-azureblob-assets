using System.IO;
using System.Threading.Tasks;

namespace VirtoCommerce.AzureBlobAssets.Abstractions
{
    public interface IAzureBlobProvider
    {
        string GetAbsoluteUrl(string blobKey);

        Stream OpenRead(string blobUrl);

        Task<Stream> OpenReadAsync(string blobUrl);

        Stream OpenWrite(string blobUrl);

        Task<Stream> OpenWriteAsync(string blobUrl);

        bool Exists(string blobUrl);

        Task<bool> ExistsAsync(string blobUrl);
    }
}
