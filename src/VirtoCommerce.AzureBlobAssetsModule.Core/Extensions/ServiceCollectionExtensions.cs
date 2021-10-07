using System;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.AssetsModule.Core.Assets;

namespace VirtoCommerce.AzureBlobAssetsModule.Core.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static void AddAzureBlobProvider(this IServiceCollection services, Action<AzureBlobOptions> setupAction = null)
        {
            services.AddSingleton<IBlobStorageProvider, AzureBlobProvider>();
            services.AddSingleton<IBlobUrlResolver, AzureBlobProvider>();
            if (setupAction != null)
            {
                services.Configure(setupAction);
            }
        }
    }
}
