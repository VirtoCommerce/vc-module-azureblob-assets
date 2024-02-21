using System.IO;
using System.Threading.Tasks;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using VirtoCommerce.Platform.Core.Common;
using Xunit;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

[Trait("Category", "IntegrationTest")]
[Collection("AzureBlobStorageProvider")]
public class AzureBlobStorageProviderIntegrationTests
{
    private readonly AzureBlobStorageProviderIntegrationTestSetup _fixture;
    private const string ContainerName = "virtualsupplier";

    public AzureBlobStorageProviderIntegrationTests(AzureBlobStorageProviderIntegrationTestSetup fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void GetAbsoluteUrl_Should_ReturnUrl()
    {
        // Arrange
        const string blobUrl = $"{ContainerName}/Catalog/result.json";

        // Act
        var uri = _fixture.Provider.GetAbsoluteUrl(blobUrl);

        // Assert
        Assert.NotEmpty(uri);
        Assert.StartsWith("https", uri);
    }

    [Fact]
    public async Task GetBlobInfo_Should_ReturnSameRelative()
    {
        // Arrange
        const string blobUrl = $"{ContainerName}/Catalog/result.json";

        // Act
        var blobInfo = await _fixture.Provider.GetBlobInfoAsync(blobUrl);

        // Assert
        Assert.NotNull(blobInfo);
        Assert.Equal(blobUrl, blobInfo.RelativeUrl, true);
    }

    [Fact]
    public async Task OpenRead_Should_ReturnNotEmptyStream()
    {
        // Arrange
        const string blobUrl = $"{ContainerName}/Catalog/result.json";

        // Act
        await using var stream = await _fixture.Provider.OpenReadAsync(blobUrl);

        // Assert
        Assert.True(stream.CanRead);
        Assert.True(stream.Length > 0);
    }

    [Fact]
    public async Task OpenWrite_Should_ReturnWritableStream()
    {
        // Arrange
        const string blobUrl = $"{ContainerName}/Catalog/temp.json";

        // Act
        await using var stream = await _fixture.Provider.OpenWriteAsync(blobUrl);
        await using var writer = new StreamWriter(stream);
        await writer.WriteAsync("""{"result":true}""");

        // Assert
        Assert.True(stream.CanWrite);
    }

    [Fact]
    public async Task Remove_Should_RemoveBlob()
    {
        // Arrange
        const string blobUrl = $"{ContainerName}/Catalog/remove.json";

        // Act
        await using (var stream = await _fixture.Provider.OpenWriteAsync(blobUrl))
        {
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync("""{"result":true}""");
        }
        var created = await _fixture.Provider.ExistsAsync(blobUrl);
        var removed = false;

        if (created)
        {
            await _fixture.Provider.RemoveAsync([blobUrl]);
            removed = !await _fixture.Provider.ExistsAsync(blobUrl);
        }

        // Assert
        Assert.True(created && removed);
    }

    [Fact]
    public async Task Move_Should_MoveBlob()
    {
        // Arrange
        const string oldBlobUrl = $"{ContainerName}/Catalog/move.json";
        const string newBlobUrl = $"{ContainerName}/Catalog/MoveFolder/move.json";

        // Act
        await using (var stream = await _fixture.Provider.OpenWriteAsync(oldBlobUrl))
        {
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync("""{"result":true}""");
        }
        var created = await _fixture.Provider.ExistsAsync(oldBlobUrl);

        var moved = false;
        if (await _fixture.Provider.ExistsAsync(newBlobUrl))
        {
            await _fixture.Provider.RemoveAsync([newBlobUrl]);
        }
        if (created)
        {
            await _fixture.Provider.MoveAsyncPublic(oldBlobUrl, newBlobUrl);
            moved = !await _fixture.Provider.ExistsAsync(oldBlobUrl) && await _fixture.Provider.ExistsAsync(newBlobUrl);
        }

        // Assert
        Assert.True(created && moved);
    }

    [Fact]
    public async Task Copy_Should_CopyBlob()
    {
        // Arrange
        const string oldBlobUrl = $"{ContainerName}/Catalog/copy.json";
        const string newBlobUrl = $"{ContainerName}/Catalog/CopyFolder/copy.json";

        // Act
        await using (var stream = await _fixture.Provider.OpenWriteAsync(oldBlobUrl))
        {
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync("""{"result":true}""");
        }
        var created = await _fixture.Provider.ExistsAsync(oldBlobUrl);

        var copied = false;
        if (await _fixture.Provider.ExistsAsync(newBlobUrl))
        {
            await _fixture.Provider.RemoveAsync([newBlobUrl]);
        }
        if (created)
        {
            await _fixture.Provider.CopyAsync(oldBlobUrl, newBlobUrl);
            copied = await _fixture.Provider.ExistsAsync(oldBlobUrl) && await _fixture.Provider.ExistsAsync(newBlobUrl);
        }

        // Assert
        Assert.True(created && copied);
    }

    [Fact]
    public async Task Search_Should_ReturnNotEmptyCollection()
    {
        // Arrange
        const string folderUrl = $"{ContainerName}/Catalog";

        // Act
        var result = await _fixture.Provider.SearchAsync(folderUrl, null);

        // Assert
        Assert.False(result?.Results.IsNullOrEmpty());
    }

    [Fact]
    public async Task SearchContainers_Should_ReturnNotEmptyCollection()
    {
        // Arrange
        const string keyword = ContainerName;

        // Act
        var result = await _fixture.Provider.SearchAsync(null, keyword);

        // Assert
        Assert.False(result?.Results.IsNullOrEmpty());
    }

    [Fact]
    public async Task CreateFolder_Should_CreateWithoutParent()
    {
        // Arrange
        const string folderUrl = $"{ContainerName}/Catalog/NewFolder";
        var folder = new BlobFolder
        {
            Name = folderUrl,
        };

        // Act
        await _fixture.Provider.CreateFolderAsync(folder);
        var created = await _fixture.Provider.ExistsAsync(folderUrl);

        // Assert
        Assert.True(created);
    }

    [Fact]
    public async Task CreateFolder_Should_CreateWithParent()
    {
        // Arrange
        const string folderUrl = $"{ContainerName}/Catalog/SubFolder";
        var folder = new BlobFolder
        {
            Name = "SubFolder",
            ParentUrl = $"{ContainerName}/Catalog",
        };

        // Act
        await _fixture.Provider.CreateFolderAsync(folder);
        var created = await _fixture.Provider.ExistsAsync(folderUrl);

        // Assert
        Assert.True(created);
    }
}

public class AzureBlobStorageProviderIntegrationTestSetup
{
    public AzureBlobProvider Provider { get; }

    public AzureBlobStorageProviderIntegrationTestSetup()
    {
        Provider = AppConfiguration.GetAzureBlobProvider();
    }
}

[CollectionDefinition("AzureBlobStorageProvider")]
public abstract class AzureBlobStorageProviderCollection : ICollectionFixture<AzureBlobStorageProviderIntegrationTestSetup>;
