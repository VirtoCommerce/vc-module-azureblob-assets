using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
