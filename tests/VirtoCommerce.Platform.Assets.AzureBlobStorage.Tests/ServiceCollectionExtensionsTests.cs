using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.Assets.Abstractions;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using VirtoCommerce.AzureBlobAssetsModule.Core.Extensions;
using VirtoCommerce.Platform.Core.Events;
using Xunit;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAzureBlobProvider_ResolvesOneProviderForEveryInterface()
    {
        using var serviceProvider = CreateServices().BuildServiceProvider();

        var provider = serviceProvider.GetRequiredService<IBlobStorageProvider>();

        Assert.IsType<AzureBlobProvider>(provider);
        Assert.Same(provider, serviceProvider.GetRequiredService<ICommonBlobProvider>());
        Assert.Same(provider, serviceProvider.GetRequiredService<IBlobUrlResolver>());
        Assert.Same(provider, serviceProvider.GetRequiredService<IAzureBlobUrlResolver>());
    }

    // A module registered later may replace one interface with its own provider, as a derived provider does
    [Fact]
    public void AddAzureBlobProvider_WhenAnInterfaceIsRegisteredAgainLater_ResolvesTheLaterRegistration()
    {
        var services = CreateServices();
        services.AddSingleton<IBlobStorageProvider, DerivedAzureBlobProvider>();
        using var serviceProvider = services.BuildServiceProvider();

        Assert.IsType<DerivedAzureBlobProvider>(serviceProvider.GetRequiredService<IBlobStorageProvider>());
        Assert.IsType<AzureBlobProvider>(serviceProvider.GetRequiredService<IBlobUrlResolver>());
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Mock.Of<IFileExtensionService>());
        services.AddSingleton(Mock.Of<IEventPublisher>());
        services.AddAzureBlobProvider(x => x.ConnectionString = "UseDevelopmentStorage=true");

        return services;
    }

    private sealed class DerivedAzureBlobProvider(IOptions<AzureBlobOptions> options, IFileExtensionService fileExtensionService, IEventPublisher eventPublisher)
        : AzureBlobProvider(options, fileExtensionService, eventPublisher);
}
