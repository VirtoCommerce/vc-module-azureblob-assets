using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using System.Linq;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using Xunit;

namespace VirtoCommerce.Platform.Assets.AzureBlobStorage.Tests
{
    public class AzureBlobStorageProviderTests
    {
        private readonly IOptions<AzureBlobOptions> _options;

        public AzureBlobStorageProviderTests()
        {
            _options = new OptionsWrapper<AzureBlobOptions>(new AppConfiguration().GetApplicationConfiguration<AzureBlobOptions>());
        }

        /// <summary>
        /// `OpenWrite` method should return write-only stream.
        /// </summary>
        /// <remarks>
        /// Broken -> https://github.com/VirtoCommerce/vc-platform/pull/2254/checks?check_run_id=2551785684
        /// </remarks>
        [Fact(Skip = "Test is broken on CI")]
        public void StreamWritePermissionsTest()
        {
            var mockFileExtensionService = new Mock<IFileExtensionService>();
            mockFileExtensionService.Setup(service => service.IsExtensionAllowedAsync(It.IsAny<string>())).ReturnsAsync(true);

            // Arrange
            var provider = new AzureBlobProvider(_options, mockFileExtensionService.Object, null);
            var fileName = "file-write.tmp";
            var fileUrl = $"tmpfolder/{fileName}";

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

        private void ValidateFailure<TOptions>(OptionsValidationException ex, string name = "", int count = 1, params string[] errorsToMatch)
        {
            Assert.Equal(typeof(TOptions), ex.OptionsType);
            Assert.Equal(name, ex.OptionsName);
            if (errorsToMatch.Length == 0)
            {
                errorsToMatch = new string[] { "A validation error has occured." };
            }
            Assert.Equal(count, ex.Failures.Count());
            // Check for the error in any of the failures
            foreach (var error in errorsToMatch)
            {
                Assert.True(ex.Failures.FirstOrDefault(f => f.Contains(error)) != null, "Did not find: " + error);
            }
        }
    }
}
