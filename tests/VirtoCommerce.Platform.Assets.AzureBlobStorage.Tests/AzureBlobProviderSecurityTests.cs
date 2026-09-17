using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using VirtoCommerce.Platform.Core.Exceptions;
using Xunit;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

/// <summary>
/// VCST-6016 (Defect 5): Move/Copy must reject a destination prefix nested beneath the
/// source. Otherwise the lazily-paged blob enumeration re-lists its own freshly-written
/// output and copies without bound (unbounded resource consumption / cost DoS).
/// </summary>
public class AzureBlobProviderSecurityTests
{
    [Fact]
    public async Task CopyAsync_WhenDestinationNestedUnderSource_ThrowsPlatformException()
    {
        var provider = CreateProvider();

        await Assert.ThrowsAsync<PlatformException>(() =>
            provider.CopyAsync("mycontainer/data/", "mycontainer/data/nested/"));
    }

    [Fact]
    public async Task MoveAsyncPublic_WhenDestinationNestedUnderSource_ThrowsPlatformException()
    {
        var provider = CreateProvider();

        await Assert.ThrowsAsync<PlatformException>(() =>
            provider.MoveAsyncPublic("mycontainer/data/", "mycontainer/data/nested/"));
    }

    // Fast, offline predicate coverage (no network): pins the nesting rule, including that Azure
    // blob names are case-sensitive so a case-only difference is NOT a nested path.
    [Theory]
    [InlineData("data/", "data/nested/", true)]        // destination nested beneath source
    [InlineData("data", "data/nested", true)]          // same, without trailing delimiters
    [InlineData("data/", "data-copy/", false)]         // sibling sharing the string prefix
    [InlineData("data/", "other/", false)]             // unrelated destination
    [InlineData("data/", "data/", false)]              // same location - not an amplification
    [InlineData("Photos/", "photos/backup/", false)]   // case differs - distinct blobs, not nested
    [InlineData("", "data/nested/", false)]            // missing source prefix
    [InlineData("data/", "", false)]                   // missing destination prefix
    public void IsDestinationNestedInSource_ReturnsExpected(string source, string destination, bool expected)
    {
        Assert.Equal(expected, AzureBlobProvider.IsDestinationNestedInSource(source, destination));
    }

    private static AzureBlobProvider CreateProvider()
    {
        var options = new AzureBlobOptions
        {
            // Points at the local emulator; the guard rejects the request before any I/O.
            ConnectionString = "UseDevelopmentStorage=true",
            AllowBlobPublicAccess = true,
        };

        var mockFileExtensionService = new Mock<IFileExtensionService>();
        mockFileExtensionService
            .Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        return new AzureBlobProvider(Options.Create(options), mockFileExtensionService.Object, eventPublisher: null);
    }
}
