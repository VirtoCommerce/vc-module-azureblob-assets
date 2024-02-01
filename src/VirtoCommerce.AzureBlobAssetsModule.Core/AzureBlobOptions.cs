using System.ComponentModel.DataAnnotations;

namespace VirtoCommerce.AzureBlobAssetsModule.Core
{
    public class AzureBlobOptions
    {
        [Required]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Url of the CDN server
        /// </summary>
        public string CdnUrl { get; set; }

        /// <summary>
        /// If true, create new blob containers with access type PublicAccessType.Blob
        /// Otherwise, PublicAccessType.None
        /// </summary>
        public bool AllowBlobPublicAccess { get; set; } = true;
    }
}
