using System.Net;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Models;
using Xunit;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

/// <summary>
/// Only a missing blob or container is "not there". Any other failure - a refused credential, throttling, an
/// unreachable account - must reach the caller instead of reporting an existing blob as missing.
/// </summary>
public class AzureBlobProviderBlobInfoTests
{
    [Theory]
    [InlineData("BlobNotFound")]
    [InlineData("ContainerNotFound")]
    public async Task GetBlobInfoAsync_WhenBlobOrContainerIsMissing_ReturnsNull(string errorCode)
    {
        using var storage = new StubBlobService(HttpStatusCode.NotFound, errorCode);
        var provider = storage.CreateProvider();

        Assert.Null(await provider.GetBlobInfoAsync("container/file.csv"));
        Assert.False(await provider.ExistsAsync("container/file.csv"));
    }

    [Fact]
    public async Task GetBlobInfoAsync_WhenCredentialIsRefused_ThrowsRequestFailedException()
    {
        using var storage = new StubBlobService(HttpStatusCode.Forbidden, BlobErrorCode.AuthorizationPermissionMismatch.ToString());
        var provider = storage.CreateProvider();

        var exception = await Assert.ThrowsAsync<RequestFailedException>(() => provider.GetBlobInfoAsync("container/file.csv"));

        Assert.Equal((int)HttpStatusCode.Forbidden, exception.Status);
    }

    [Fact]
    public async Task ExistsAsync_WhenCredentialIsRefused_ThrowsRequestFailedException()
    {
        using var storage = new StubBlobService(HttpStatusCode.Forbidden, BlobErrorCode.AuthorizationPermissionMismatch.ToString());
        var provider = storage.CreateProvider();

        var exception = await Assert.ThrowsAsync<RequestFailedException>(() => provider.ExistsAsync("container/file.csv"));

        Assert.Equal((int)HttpStatusCode.Forbidden, exception.Status);
    }
}
