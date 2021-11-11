using Microsoft.Extensions.DependencyInjection;
using System;
using VirtoCommerce.AssetsModule.Core.Assets;
using VirtoCommerce.AzureBlobAssets.Abstractions;

namespace VirtoCommerce.AzureBlobAssetsModule.Core.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static void AddAzureBlobProvider(this IServiceCollection services, Action<AzureBlobOptions> setupAction = null)
        {
            services.AddSingleton<IAzureBlobProvider, AzureBlobProvider>();
            services.AddSingleton<IBlobStorageProvider, AzureBlobProvider>();
            services.AddSingleton<IBlobUrlResolver, AzureBlobProvider>();
            if (setupAction != null)
            {
                services.Configure(setupAction);
            }
        }
    }
}
