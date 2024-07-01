using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using Xunit;

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
        var options = new AzureBlobOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
            CdnUrl = "",
            AllowBlobPublicAccess = true,
        };

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
        var options = new AzureBlobOptions
        {
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=qademovc3;AccountKey=;EndpointSuffix=core.windows.net",
            CdnUrl = "cdn.mydomain.com",
            AllowBlobPublicAccess = true,
        };

        var mockFileExtensionService = new Mock<IFileExtensionService>();
        mockFileExtensionService
            .Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        var provider = new AzureBlobProvider(Options.Create(options), mockFileExtensionService.Object, eventPublisher: null);
        var blobUrlResolver = (IBlobUrlResolver)provider;

        Assert.Equal(absoluteUrl, blobUrlResolver.GetAbsoluteUrl(blobKey));
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
