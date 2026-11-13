using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

/// <summary>
/// Which blob a url addresses. Azure Storage takes a backslash as part of a blob name, with "/" the only delimiter
/// (https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata:
/// "A blob name can contain any combination of characters"), so an encoded backslash stays in the name. A backslash
/// written out in a url is a separator, as it always was.
/// </summary>
public class AzureBlobProviderBlobNameTests
{
    [Theory]
    [InlineData("https://account.blob.core.windows.net/c/f/a%5Cb.csv", "/devstoreaccount1/c/f/a%5Cb.csv")]
    [InlineData("https://account.blob.core.windows.net/c/f/a%5cb.csv", "/devstoreaccount1/c/f/a%5Cb.csv")]
    [InlineData("c/f/a%5Cb.csv", "/devstoreaccount1/c/f/a%5Cb.csv")]
    [InlineData("/c/f/a%5Cb.csv", "/devstoreaccount1/c/f/a%5Cb.csv")]
    [InlineData("c\\f\\a.csv", "/devstoreaccount1/c/f/a.csv")]
    public async Task GetBlobInfoAsync_RequestsTheBlobTheUrlNames(string url, string expectedPath)
    {
        using var storage = new StubBlobService(HttpStatusCode.NotFound, "BlobNotFound");
        var provider = storage.CreateProvider();

        await provider.GetBlobInfoAsync(url);

        Assert.Equal($"HEAD {expectedPath} HTTP/1.1", Assert.Single(storage.RequestLines));
    }
}
