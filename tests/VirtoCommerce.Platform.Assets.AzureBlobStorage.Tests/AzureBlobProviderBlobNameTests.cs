using System.Linq;
using System.Net;
using System.Threading.Tasks;
using VirtoCommerce.AssetsModule.Core.Assets;
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
    // An absolute url need not be well-formed to be absolute: a space, a "\" or a bracket written out
    [InlineData("https://account.blob.core.windows.net/c/f/a b.csv", "/devstoreaccount1/c/f/a%20b.csv")]
    [InlineData("https://account.blob.core.windows.net/c/f\\a.csv", "/devstoreaccount1/c/f/a.csv")]
    [InlineData("https://account.blob.core.windows.net/c/f/a[1].csv", "/devstoreaccount1/c/f/a%5B1%5D.csv")]
    public async Task GetBlobInfoAsync_RequestsTheBlobTheUrlNames(string url, string expectedPath)
    {
        using var storage = new StubBlobService(HttpStatusCode.NotFound, "BlobNotFound");
        var provider = storage.CreateProvider();

        await provider.GetBlobInfoAsync(url);

        Assert.Equal($"HEAD {expectedPath} HTTP/1.1", Assert.Single(storage.RequestLines));
    }

    [Fact]
    public async Task CreateFolderAsync_WhenParentUrlIsAbsoluteWithSpace_RequestsWhatItsEncodedFormDoes()
    {
        using var storage = new StubBlobService(HttpStatusCode.NotFound, "ContainerNotFound");
        var provider = storage.CreateProvider();

        await Record.ExceptionAsync(() => provider.CreateFolderAsync(new BlobFolder { Name = "new", ParentUrl = $"{storage.BlobEndpoint}/c/my%20folder" }));
        var encodedFormRequests = storage.RequestLines.ToArray();
        storage.RequestLines.Clear();

        await Record.ExceptionAsync(() => provider.CreateFolderAsync(new BlobFolder { Name = "new", ParentUrl = $"{storage.BlobEndpoint}/c/my folder" }));

        Assert.NotEmpty(encodedFormRequests);
        Assert.Equal(encodedFormRequests, storage.RequestLines.ToArray());
    }
}
