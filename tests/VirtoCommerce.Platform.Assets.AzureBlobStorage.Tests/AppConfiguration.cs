using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using VirtoCommerce.Platform.Core;
using VirtoCommerce.Platform.Core.Settings;

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
        var platformOptions = Options.Create(new PlatformOptions
        {
            FileExtensionsBlackList = [],
            FileExtensionsWhiteList = [],
        });
        var settingsManager = new Mock<ISettingsManager>();
        settingsManager.Setup(x => x.GetObjectSettingAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(new ObjectSettingEntry { AllowedValues = [] });
        var fileExtensionService = new FileExtensionService(platformOptions, settingsManager.Object);

        return new AzureBlobProvider(options, fileExtensionService);
    }
}
