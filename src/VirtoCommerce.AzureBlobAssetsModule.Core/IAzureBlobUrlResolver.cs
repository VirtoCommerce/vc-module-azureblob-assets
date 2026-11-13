using System;

namespace VirtoCommerce.AzureBlobAssetsModule.Core;

public interface IAzureBlobUrlResolver
{
    /// <summary>
    /// True when <see cref="GenerateSasUrl"/> can sign a url: the connection holds an account key or is a service
    /// principal. A connection by shared access signature signs nothing.
    /// </summary>
    /// <remarks>
    /// True by default, so an implementation written before this member keeps compiling and keeps its behaviour.
    /// </remarks>
    bool CanGenerateSasUrl => true;

    string GenerateSasUrl(string blobUrl, TimeSpan expiresIn);
}
