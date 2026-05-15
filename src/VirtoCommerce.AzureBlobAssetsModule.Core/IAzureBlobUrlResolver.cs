using System;

namespace VirtoCommerce.AzureBlobAssetsModule.Core;

public interface IAzureBlobUrlResolver
{
    string GenerateSasUrl(string blobUrl, TimeSpan expiresIn);
}
