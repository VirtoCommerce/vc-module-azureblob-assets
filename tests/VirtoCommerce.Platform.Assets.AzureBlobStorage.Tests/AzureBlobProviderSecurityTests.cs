using System;
using System.Net;
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
/// A blob url must not reach outside the container it names through a "." or ".." segment.
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

    // A "." or ".." segment in a blob url leaves the container it names: the request uri is normalized, so container
    // "c" with blob "x/../../other/s.csv" becomes a request for "/other/s.csv". The url is refused before any request,
    // whether the segment is written out, percent-encoded, or the container itself.
    public static TheoryData<string> DotSegmentUrls =>
    [
        "c/x/../../other/s.csv",
        "/c/x/../../other/s.csv",
        "c/x/%2e%2e/%2E%2E/other/s.csv",
        "https://account.blob.core.windows.net/c/x/%2e%2e/%2e%2e/other/s.csv",
        // The path of an absolute url is decoded twice on its way to a blob name
        "https://account.blob.core.windows.net/c/x/%252e%252e/%252e%252e/other/s.csv",
        "c/./s.csv",
        "../other/s.csv",
        "./c/s.csv",
    ];

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task OpenReadAsync_WhenUrlHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.OpenReadAsync(url));
    }

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task OpenWriteAsync_WhenUrlHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.OpenWriteAsync(url));
    }

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task GetBlobInfoAsync_WhenUrlHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.GetBlobInfoAsync(url));
    }

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task RemoveAsync_WhenUrlHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.RemoveAsync([url]));
    }

    [Fact]
    public Task RemoveAsync_WhenAnyUrlHasDotSegment_DeletesNothing()
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.RemoveAsync(["c/kept.csv", "c/x/../../other/s.csv"]));
    }

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task CopyAsync_WhenSourceHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.CopyAsync(url, "c/copy.csv"));
    }

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task CopyAsync_WhenDestinationHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.CopyAsync("c/s.csv", url));
    }

    [Theory]
    [MemberData(nameof(DotSegmentUrls))]
    public Task MoveAsyncPublic_WhenDestinationHasDotSegment_ThrowsBeforeAnyRequest(string url)
    {
        return AssertRefusedBeforeAnyRequest(provider => provider.MoveAsyncPublic("c/s.csv", url));
    }

    [Theory]
    [InlineData("c/x/..data/s.csv")]
    [InlineData("c/x/data../s.csv")]
    [InlineData("c/x/.keep")]
    [InlineData("c/x/.../s.csv")]
    public async Task GetBlobInfoAsync_WhenDotsArePartOfAName_SendsTheRequest(string url)
    {
        using var storage = new StubBlobService(HttpStatusCode.NotFound, "BlobNotFound");
        var provider = storage.CreateProvider();

        Assert.Null(await provider.GetBlobInfoAsync(url));
        Assert.Single(storage.RequestLines);
    }

    private static async Task AssertRefusedBeforeAnyRequest(Func<AzureBlobProvider, Task> operation)
    {
        using var storage = new StubBlobService(HttpStatusCode.NotFound, "BlobNotFound");
        var provider = storage.CreateProvider();

        var exception = await Record.ExceptionAsync(() => operation(provider));

        Assert.Empty(storage.RequestLines);
        Assert.IsType<ArgumentException>(exception);
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
