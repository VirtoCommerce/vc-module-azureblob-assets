using System;
using System.ComponentModel.DataAnnotations;

namespace VirtoCommerce.AzureBlobAssetsModule.Core
{
    public class AzureBlobOptions
    {
        [Required]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Public URL used to expose blobs to clients (admin UI, storefront).
        /// Accepts a bare host (cdn.example.com), a full URL (https://cdn.example.com),
        /// or a full URL with a sub-path (https://cdn.example.com/static).
        /// </summary>
        public string PublicUrl { get; set; }

        /// <summary>
        /// Url of the CDN server. Alias of <see cref="PublicUrl"/> kept for backward compatibility.
        /// </summary>
        [Obsolete("Use PublicUrl instead. CdnUrl is kept as an alias for backward compatibility.")]
        public string CdnUrl
        {
            get => PublicUrl;
            set => PublicUrl = value;
        }

        /// <summary>
        /// If true, create new blob containers with access type PublicAccessType.Blob
        /// Otherwise, PublicAccessType.None
        /// </summary>
        public bool AllowBlobPublicAccess { get; set; } = true;
    }
}
