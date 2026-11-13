using System;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.Assets.Abstractions;
using VirtoCommerce.AssetsModule.Core.Assets;

namespace VirtoCommerce.AzureBlobAssetsModule.Core.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static void AddAzureBlobProvider(this IServiceCollection services, Action<AzureBlobOptions> setupAction = null)
        {
            // One provider behind every interface: each instance would hold a client of its own
            services.AddSingleton<AzureBlobProvider>();
            services.AddSingleton<ICommonBlobProvider>(x => x.GetRequiredService<AzureBlobProvider>());
            services.AddSingleton<IBlobStorageProvider>(x => x.GetRequiredService<AzureBlobProvider>());
            services.AddSingleton<IBlobUrlResolver>(x => x.GetRequiredService<AzureBlobProvider>());
            services.AddSingleton<IAzureBlobUrlResolver>(x => x.GetRequiredService<AzureBlobProvider>());
            if (setupAction != null)
            {
                services.Configure(setupAction);
            }
        }
    }
}
