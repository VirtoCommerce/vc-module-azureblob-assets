using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

public class AppConfiguration
{
    private static IConfigurationRoot _configuration;

    public AppConfiguration()
    {
        // Build configuration
        _configuration = new ConfigurationBuilder()
            .AddUserSecrets<AppConfiguration>()
            .Build();
    }

    public T GetApplicationConfiguration<T>()
        where T : new()
    {
        var result = new T();
        _configuration.GetSection("Assets:AzureBlobStorage").Bind(result);

        return result;
    }

    public static AzureBlobProvider GetAzureBlobProvider()
    {
        var options = Options.Create(new AppConfiguration().GetApplicationConfiguration<AzureBlobOptions>());

        var mockFileExtensionService = new Mock<IFileExtensionService>();
        mockFileExtensionService
            .Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        return new AzureBlobProvider(options, mockFileExtensionService.Object, eventPublisher: null);
    }
}
