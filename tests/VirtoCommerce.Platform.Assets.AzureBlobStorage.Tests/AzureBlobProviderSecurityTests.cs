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
