using System;
using System.Linq;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using Xunit;
using BlobInfo = VirtoCommerce.AssetsModule.Core.Assets.BlobInfo;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

public class AzureBlobStorageProviderTests
{
    /// <summary>
    /// `OpenWrite` method should return write-only stream.
    /// </summary>
    /// <remarks>
    /// Broken -> https://github.com/VirtoCommerce/vc-platform/pull/2254/checks?check_run_id=2551785684
    /// </remarks>
    [Fact(Skip = "Test is broken on CI")]
    public void StreamWritePermissionsTest()
    {
        // Arrange
        var provider = AppConfiguration.GetAzureBlobProvider();
        const string fileUrl = "tmpfolder/file-write.tmp";

        // Act
        using var actualStream = provider.OpenWrite(fileUrl);

        // Assert
        Assert.True(actualStream.CanWrite, "'OpenWrite' stream should be writable.");
        Assert.False(actualStream.CanRead, "'OpenWrite' stream should be write-only.");
        Assert.Equal(0, actualStream.Position);
    }

    [Fact]
    public void AzureBlobOptions_CanValidateDataAnnotations()
    {
        //Arrange
        var services = new ServiceCollection();
        services.AddOptions<AzureBlobOptions>()
            .Configure(o =>
            {
                o.ConnectionString = null;
            })
            .ValidateDataAnnotations();

        //Act
        var sp = services.BuildServiceProvider();

        //Assert
        var error = Assert.Throws<OptionsValidationException>(() => sp.GetRequiredService<IOptions<AzureBlobOptions>>().Value);
        ValidateFailure<AzureBlobOptions>(error, Options.DefaultName, 1,
            $"DataAnnotation validation failed for '{nameof(AzureBlobOptions)}' members: '{nameof(AzureBlobOptions.ConnectionString)}' with the error: 'The {nameof(AzureBlobOptions.ConnectionString)} field is required.'.");
    }

    [Theory]
    [InlineData("catalog/151349/epsonprinter.txt", "https://qademovc3.blob.core.windows.net/catalog/151349/epsonprinter.txt")]
    [InlineData("catalog/151349/epson printer.txt", "https://qademovc3.blob.core.windows.net/catalog/151349/epson%20printer.txt")]
    [InlineData("catalog/151349/epson%20printer.txt?test=Name%20With%20Space", "https://qademovc3.blob.core.windows.net/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("catalog/151349/epson printer.txt?test=Name With Space", "https://qademovc3.blob.core.windows.net/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/catalog/151349/epsonprinter.txt", "https://qademovc3.blob.core.windows.net/catalog/151349/epsonprinter.txt")]
    [InlineData("/catalog/151349/epson printer.txt", "https://qademovc3.blob.core.windows.net/catalog/151349/epson%20printer.txt")]
    [InlineData("/catalog/151349/epson%20printer.txt?test=Name%20With%20Space", "https://qademovc3.blob.core.windows.net/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/catalog/151349/epson printer.txt?test=Name With Space", "https://qademovc3.blob.core.windows.net/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("epsonprinter.txt", "https://qademovc3.blob.core.windows.net/epsonprinter.txt")]
    [InlineData("/epson printer.txt", "https://qademovc3.blob.core.windows.net/epson%20printer.txt")]
    [InlineData("epson%20printer.txt?test=Name%20With%20Space", "https://qademovc3.blob.core.windows.net/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/epson printer.txt?test=Name With Space", "https://qademovc3.blob.core.windows.net/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/epson%20printer.txt?test=Name+With+Space", "https://qademovc3.blob.core.windows.net/epson%20printer.txt?test=Name+With+Space")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epson printer.txt?test=Name With Space", "https://localhost:5001/assets/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epsonprinter.txt", "https://localhost:5001/assets/catalog/151349/epsonprinter.txt")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epson printer.txt", "https://localhost:5001/assets/catalog/151349/epson%20printer.txt")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epson%20printer.txt?test=Name%20With%20Space", "https://localhost:5001/assets/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    public void GetAbsoluteUrlTest(string blobKey, string absoluteUrl)
    {
#pragma warning disable CS0618 // legacy test exercises the CdnUrl alias intentionally
        var options = new AzureBlobOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
            CdnUrl = "",
            AllowBlobPublicAccess = true,
        };
#pragma warning restore CS0618

        var mockFileExtensionService = new Mock<IFileExtensionService>();
        mockFileExtensionService
            .Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        var provider = new AzureBlobProvider(Options.Create(options), mockFileExtensionService.Object, eventPublisher: null);
        var blobUrlResolver = (IBlobUrlResolver)provider;

        Assert.Equal(absoluteUrl, blobUrlResolver.GetAbsoluteUrl(blobKey));
    }

    [Theory]
    [InlineData("catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/catalog/151349/epsonprinter.txt")]
    [InlineData("catalog/151349/epson printer.txt", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt")]
    [InlineData("catalog/151349/epson%20printer.txt?test=Name%20With%20Space", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("catalog/151349/epson printer.txt?test=Name With Space", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/catalog/151349/epsonprinter.txt")]
    [InlineData("/catalog/151349/epson printer.txt", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt")]
    [InlineData("/catalog/151349/epson%20printer.txt?test=Name%20With%20Space", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/catalog/151349/epson printer.txt?test=Name With Space", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("epsonprinter.txt", "https://cdn.mydomain.com/epsonprinter.txt")]
    [InlineData("/epson printer.txt", "https://cdn.mydomain.com/epson%20printer.txt")]
    [InlineData("epson%20printer.txt?test=Name%20With%20Space", "https://cdn.mydomain.com/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/epson printer.txt?test=Name With Space", "https://cdn.mydomain.com/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("/epson%20printer.txt?test=Name+With+Space", "https://cdn.mydomain.com/epson%20printer.txt?test=Name+With+Space")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epson printer.txt?test=Name With Space", "https://localhost:5001/assets/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epsonprinter.txt", "https://localhost:5001/assets/catalog/151349/epsonprinter.txt")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epson printer.txt", "https://localhost:5001/assets/catalog/151349/epson%20printer.txt")]
    [InlineData("https://localhost:5001/assets/catalog/151349/epson%20printer.txt?test=Name%20With%20Space", "https://localhost:5001/assets/catalog/151349/epson%20printer.txt?test=Name%20With%20Space")]
    public void GetAbsoluteUrlTestWithCdn(string blobKey, string absoluteUrl)
    {
#pragma warning disable CS0618 // legacy test exercises the CdnUrl alias intentionally
        var options = new AzureBlobOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
            CdnUrl = "cdn.mydomain.com",
            AllowBlobPublicAccess = true,
        };
#pragma warning restore CS0618

        var mockFileExtensionService = new Mock<IFileExtensionService>();
        mockFileExtensionService
            .Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        var provider = new AzureBlobProvider(Options.Create(options), mockFileExtensionService.Object, eventPublisher: null);
        var blobUrlResolver = (IBlobUrlResolver)provider;

        Assert.Equal(absoluteUrl, blobUrlResolver.GetAbsoluteUrl(blobKey));
    }

    [Theory]
    [InlineData("https://qademovc3.core.windows.net/cms", "Themes/", "https://qademovc3.core.windows.net/cms/Themes/")]
    [InlineData("https://qademovc3.core.windows.net/cms/", "Themes", "https://qademovc3.core.windows.net/cms/Themes")]
    [InlineData("https://qademovc3.core.windows.net/cms/", "/Themes/", "https://qademovc3.core.windows.net/cms/Themes/")]
    [InlineData("https://qademovc3.core.windows.net/cms", "/Themes", "https://qademovc3.core.windows.net/cms/Themes")]
    [InlineData("https://qademovc3.core.windows.net/", "Themes/", "https://qademovc3.core.windows.net/Themes/")]
    [InlineData("https://qademovc3.core.windows.net", "Themes/", "https://qademovc3.core.windows.net/Themes/")]
    public void GetAbsoluteUri_StaticMethod(string baseUrl, string inputUrl, string absoluteUrl)
    {
        Assert.Equal(absoluteUrl, AzureBlobProvider.GetAbsoluteUri(new Uri(baseUrl), inputUrl).AbsoluteUri);
    }

    [Theory]
    [InlineData("https://qademovc3.core.windows.net/cms/test?sv=2022-11-02&ss=b&srt=co&sp", "Catalog/", "https://qademovc3.core.windows.net/cms/test/Catalog/?sv=2022-11-02&ss=b&srt=co&sp")]
    [InlineData("https://qademovc3.core.windows.net/cms/test?sv=2022-11-02&ss=b&srt=co&sp", "/Catalog", "https://qademovc3.core.windows.net/cms/test/Catalog?sv=2022-11-02&ss=b&srt=co&sp")]
    [InlineData("https://qademovc3.core.windows.net/cms/test?sv=2022-11-02&ss=b&srt=co&sp", "/Catalog/", "https://qademovc3.core.windows.net/cms/test/Catalog/?sv=2022-11-02&ss=b&srt=co&sp")]
    public void GetAbsoluteUriWithParameters_StaticMethod(string baseUrl, string inputUrl, string absoluteUrl)
    {
        Assert.Equal(absoluteUrl, AzureBlobProvider.GetAbsoluteUri(new Uri(baseUrl), inputUrl).AbsoluteUri);
    }

    [Theory]
    [InlineData("catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/catalog/151349/epsonprinter.txt")]
    [InlineData("catalog/151349/epson printer.txt", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt")]
    [InlineData("/catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/catalog/151349/epsonprinter.txt")]
    [InlineData("epsonprinter.txt", "https://cdn.mydomain.com/epsonprinter.txt")]
    public void GetAbsoluteUrlTestWithPublicUrl(string blobKey, string absoluteUrl)
    {
        // PublicUrl as bare host — should behave identically to legacy CdnUrl bare-host case
        var provider = CreateProvider(publicUrl: "cdn.mydomain.com");
        var blobUrlResolver = (IBlobUrlResolver)provider;

        Assert.Equal(absoluteUrl, blobUrlResolver.GetAbsoluteUrl(blobKey));
    }

    [Theory]
    [InlineData("catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/catalog/151349/epsonprinter.txt")]
    [InlineData("catalog/151349/epson printer.txt", "https://cdn.mydomain.com/catalog/151349/epson%20printer.txt")]
    [InlineData("/catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/catalog/151349/epsonprinter.txt")]
    [InlineData("epsonprinter.txt", "https://cdn.mydomain.com/epsonprinter.txt")]
    public void GetAbsoluteUrlTestWithPublicUrlFullUrl(string blobKey, string absoluteUrl)
    {
        // PublicUrl supplied as a full URL with scheme — must produce the same output as bare host
        var provider = CreateProvider(publicUrl: "https://cdn.mydomain.com");
        var blobUrlResolver = (IBlobUrlResolver)provider;

        Assert.Equal(absoluteUrl, blobUrlResolver.GetAbsoluteUrl(blobKey));
    }

    [Theory]
    [InlineData("catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/static/catalog/151349/epsonprinter.txt")]
    [InlineData("catalog/151349/epson printer.txt", "https://cdn.mydomain.com/static/catalog/151349/epson%20printer.txt")]
    [InlineData("/catalog/151349/epsonprinter.txt", "https://cdn.mydomain.com/static/catalog/151349/epsonprinter.txt")]
    public void GetAbsoluteUrlTestWithPublicUrlAndPath(string blobKey, string absoluteUrl)
    {
        // PublicUrl with a sub-path — must be preserved as a prefix
        var provider = CreateProvider(publicUrl: "https://cdn.mydomain.com/static");
        var blobUrlResolver = (IBlobUrlResolver)provider;

        Assert.Equal(absoluteUrl, blobUrlResolver.GetAbsoluteUrl(blobKey));
    }

    [Fact]
    public void AzureBlobOptions_CdnUrlAliasesPublicUrl()
    {
#pragma warning disable CS0618 // exercising the obsolete alias intentionally
        var fromCdn = new AzureBlobOptions { CdnUrl = "x.example.com" };
        Assert.Equal("x.example.com", fromCdn.PublicUrl);

        var fromPublic = new AzureBlobOptions { PublicUrl = "y.example.com" };
        Assert.Equal("y.example.com", fromPublic.CdnUrl);
#pragma warning restore CS0618
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AzureBlobOptions_CdnUrlSetter_DoesNotClearPublicUrl_WhenValueIsNullOrEmpty(string emptyValue)
    {
        // Guards against the configuration binder calling CdnUrl=null after PublicUrl
        // has been bound (which would otherwise silently wipe out PublicUrl).
        var options = new AzureBlobOptions { PublicUrl = "https://cdn.example.com" };

#pragma warning disable CS0618 // exercising the obsolete alias intentionally
        options.CdnUrl = emptyValue;
#pragma warning restore CS0618

        Assert.Equal("https://cdn.example.com", options.PublicUrl);
    }

    [Fact]
    public void LegacyCdnUrlConfig_StillRoutedThroughPublicUrl()
    {
        // Configs that still use the legacy CdnUrl key must keep working.
#pragma warning disable CS0618
        var options = new AzureBlobOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
            CdnUrl = "https://legacy.example.com",
            AllowBlobPublicAccess = true,
        };
#pragma warning restore CS0618

        var provider = CreateProvider(options: options);
        var url = ((IBlobUrlResolver)provider).GetAbsoluteUrl("file.txt");

        Assert.Equal("https://legacy.example.com/file.txt", url);
    }

    [Fact]
    public void ConvertToBlobInfo_UsesPublicUrl_WhenConfigured()
    {
        var provider = new TestableAzureBlobProvider(publicUrl: "https://cdn.example.com");
        var publicBaseUri = new Uri("https://cdn.example.com/catalog/");
        var internalBaseUri = new Uri("https://qademovc3.blob.core.windows.net/catalog/");

        var blobItem = BlobsModelFactory.BlobItem(
            name: "folder/file.txt",
            deleted: false,
            properties: BlobsModelFactory.BlobItemProperties(
                accessTierInferred: false,
                contentType: "text/plain",
                contentLength: 123,
                createdOn: new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
                lastModified: new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero)));

        var blobInfo = provider.InvokeConvertToBlobInfo(blobItem, publicBaseUri, internalBaseUri);

        Assert.Equal("https://cdn.example.com/catalog/folder/file.txt", blobInfo.Url);
        Assert.DoesNotContain("cdn.example.com", blobInfo.RelativeUrl);
        Assert.StartsWith("/", blobInfo.RelativeUrl);
    }

    [Fact]
    public void ConvertToBlobInfo_FallsBackToBlobUri_WhenPublicUrlEmpty()
    {
        var provider = new TestableAzureBlobProvider(publicUrl: null);
        var blobServiceUri = new Uri("https://qademovc3.blob.core.windows.net/catalog/");

        var blobItem = BlobsModelFactory.BlobItem(
            name: "folder/file.txt",
            deleted: false,
            properties: BlobsModelFactory.BlobItemProperties(
                accessTierInferred: false,
                contentType: "text/plain",
                contentLength: 123,
                createdOn: new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
                lastModified: new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero)));

        var blobInfo = provider.InvokeConvertToBlobInfo(blobItem, blobServiceUri, blobServiceUri);

        Assert.StartsWith("https://qademovc3.blob.core.windows.net/", blobInfo.Url);
    }

    [Fact]
    public void ConvertToBlobFolder_TopLevelContainer_UsesPublicUrl()
    {
        var provider = new TestableAzureBlobProvider(publicUrl: "https://cdn.example.com");

        var container = BlobsModelFactory.BlobContainerItem(
            name: "catalog",
            properties: BlobsModelFactory.BlobContainerProperties(
                lastModified: new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
                eTag: new Azure.ETag("etag")));

        var folder = provider.InvokeConvertToBlobFolder(container);

        Assert.StartsWith("https://cdn.example.com/catalog", folder.Url);
        Assert.DoesNotContain("cdn.example.com", folder.RelativeUrl);
    }

    private static AzureBlobProvider CreateProvider(string publicUrl = "", AzureBlobOptions options = null)
    {
        options ??= new AzureBlobOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
            PublicUrl = publicUrl,
            AllowBlobPublicAccess = true,
        };

        var mockFileExtensionService = new Mock<IFileExtensionService>();
        mockFileExtensionService
            .Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        return new AzureBlobProvider(Options.Create(options), mockFileExtensionService.Object, eventPublisher: null);
    }

    private sealed class TestableAzureBlobProvider : AzureBlobProvider
    {
        public TestableAzureBlobProvider(string publicUrl)
            : base(Options.Create(new AzureBlobOptions
            {
                ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
                PublicUrl = publicUrl,
                AllowBlobPublicAccess = true,
            }), CreateFileExtensionService(), eventPublisher: null)
        {
        }

        public BlobInfo InvokeConvertToBlobInfo(BlobItem blob, Uri publicBaseUri, Uri internalBaseUri)
        {
            return ConvertToBlobInfo(blob, publicBaseUri, internalBaseUri);
        }

        public BlobFolder InvokeConvertToBlobFolder(BlobContainerItem container)
        {
            return ConvertToBlobFolder(container);
        }

        private static IFileExtensionService CreateFileExtensionService()
        {
            var mock = new Mock<IFileExtensionService>();
            mock.Setup(s => s.IsExtensionAllowedAsync(It.IsAny<string>())).ReturnsAsync(true);
            return mock.Object;
        }
    }

    private static void ValidateFailure<TOptions>(OptionsValidationException ex, string name = "", int count = 1, params string[] errorsToMatch)
    {
        Assert.Equal(typeof(TOptions), ex.OptionsType);
        Assert.Equal(name, ex.OptionsName);
        if (errorsToMatch.Length == 0)
        {
            errorsToMatch = ["A validation error has occured."];
        }
        Assert.Equal(count, ex.Failures.Count());
        // Check for the error in any of the failures
        foreach (var error in errorsToMatch)
        {
            Assert.True(ex.Failures.FirstOrDefault(f => f.Contains(error)) != null, "Did not find: " + error);
        }
    }
}
