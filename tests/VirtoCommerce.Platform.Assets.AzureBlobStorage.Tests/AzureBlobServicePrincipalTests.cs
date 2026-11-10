using System;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;
using Xunit;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

/// <summary>
/// A service principal connection string, and the urls a provider over it signs with a user delegation key.
/// Offline: the key request is the only call to the storage account and is substituted.
/// </summary>
public class AzureBlobServicePrincipalTests
{
    private const string ClientSecret = "not-a-real-secret";
    private const string ServicePrincipalConnectionString =
        "BlobEndpoint=https://myaccount.blob.core.windows.net;TenantId=00000000-0000-0000-0000-000000000001;ClientId=00000000-0000-0000-0000-000000000002;ClientSecret=" + ClientSecret;
    private const string SasConnectionString =
        "BlobEndpoint=https://myaccount.blob.core.windows.net;SharedAccessSignature=sv=2022-11-02&ss=b&srt=co&sp=rl&se=2099-01-01T00:00:00Z&sig=c2ln";
    private const string BlobUrl = "https://myaccount.blob.core.windows.net/mycontainer/folder/file.csv";
    private const string KeyObjectId = "00000000-0000-0000-0000-000000000003";

    private static readonly string _accountKeyConnectionString =
        $"DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey={Convert.ToBase64String(new byte[64])};EndpointSuffix=core.windows.net";

    [Fact]
    public void CreateServiceClient_ServicePrincipal_AddressesTheBlobEndpointWithoutAKey()
    {
        var client = AzureBlobConnectionString.CreateServiceClient(ServicePrincipalConnectionString);

        Assert.Equal(new Uri("https://myaccount.blob.core.windows.net"), client.Uri);
        Assert.Equal("myaccount", client.AccountName);
        Assert.False(client.CanGenerateAccountSasUri);
    }

    [Fact]
    public void IsServicePrincipal_EachConnectionForm_IsTrueForTheServicePrincipalOnly()
    {
        Assert.True(AzureBlobConnectionString.IsServicePrincipal(ServicePrincipalConnectionString));
        Assert.False(AzureBlobConnectionString.IsServicePrincipal(_accountKeyConnectionString));
        Assert.False(AzureBlobConnectionString.IsServicePrincipal(SasConnectionString));
    }

    [Theory]
    [InlineData("TenantId")]
    [InlineData("ClientId")]
    [InlineData("ClientSecret")]
    [InlineData("BlobEndpoint")]
    public void CreateServiceClient_ServicePrincipalKeyMissing_NamesTheKeyAndNeverTheSecret(string missingKey)
    {
        var connectionString = string.Join(';', Array.FindAll(
            ServicePrincipalConnectionString.Split(';'),
            x => !x.StartsWith(missingKey + "=", StringComparison.Ordinal)));

        var error = Assert.Throws<ArgumentException>(() => AzureBlobConnectionString.CreateServiceClient(connectionString));

        Assert.Contains($"{missingKey} is missing", error.Message);
        Assert.DoesNotContain(ClientSecret, error.Message);
    }

    [Fact]
    public void CreateServiceClient_ServicePrincipalEndpointNotAbsolute_Throws()
    {
        var connectionString = ServicePrincipalConnectionString.Replace("https://myaccount.blob.core.windows.net", "myaccount");

        var error = Assert.Throws<ArgumentException>(() => AzureBlobConnectionString.CreateServiceClient(connectionString));

        Assert.DoesNotContain(ClientSecret, error.Message);
    }

    [Fact]
    public void CanGenerateSasUrl_EachConnectionForm_IsTrueForAnAccountKeyAndAServicePrincipal()
    {
        Assert.True(CreateProvider(_accountKeyConnectionString).CanGenerateSasUrl);
        Assert.True(CreateProvider(ServicePrincipalConnectionString).CanGenerateSasUrl);
        Assert.False(CreateProvider(SasConnectionString).CanGenerateSasUrl);
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipal_SignsTheBlobReadOnlyWithAUserDelegationKey()
    {
        var provider = new FakeKeyProvider();

        var signedUrl = provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(1));

        var builder = new BlobUriBuilder(new Uri(signedUrl));
        Assert.Equal("mycontainer", builder.BlobContainerName);
        Assert.Equal("folder/file.csv", builder.BlobName);
        Assert.Equal("r", builder.Sas.Permissions);
        Assert.Equal("b", builder.Sas.Resource);
        Assert.Equal(KeyObjectId, builder.Sas.KeyObjectId);
        Assert.False(string.IsNullOrEmpty(builder.Sas.Signature));
        Assert.InRange(builder.Sas.ExpiresOn, DateTimeOffset.UtcNow.AddDays(1).AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1).AddMinutes(1));
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalSeveralUrls_RequestsOneKey()
    {
        var provider = new FakeKeyProvider();

        //The key is requested for seven days, so it covers any url that expires within them
        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(1));
        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(2));

        Assert.Equal(1, provider.KeyRequests);
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalKeyExpiresBeforeTheUrl_RequestsANewKey()
    {
        var provider = new FakeKeyProvider { KeyLifetime = TimeSpan.FromHours(1) };

        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromMinutes(30));
        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(1));

        Assert.Equal(2, provider.KeyRequests);
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalLongerThanSevenDays_Throws()
    {
        var provider = new FakeKeyProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() => provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(7).Add(TimeSpan.FromSeconds(1))));
        Assert.Equal(0, provider.KeyRequests);
    }

    [Fact]
    public void GenerateSasUrl_AccountKeyLongerThanSevenDays_SignsAsBefore()
    {
        var provider = CreateProvider(_accountKeyConnectionString);

        var signedUrl = provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(30));

        Assert.Null(new BlobUriBuilder(new Uri(signedUrl)).Sas.KeyObjectId);
    }

    private static AzureBlobProvider CreateProvider(string connectionString)
    {
        return new AzureBlobProvider(CreateOptions(connectionString), Mock.Of<IFileExtensionService>(), null);
    }

    private static IOptions<AzureBlobOptions> CreateOptions(string connectionString)
    {
        return Options.Create(new AzureBlobOptions { ConnectionString = connectionString });
    }

    private sealed class FakeKeyProvider() : AzureBlobProvider(CreateOptions(ServicePrincipalConnectionString), Mock.Of<IFileExtensionService>(), null)
    {
        public int KeyRequests { get; private set; }

        // Null: the key lasts as long as requested
        public TimeSpan? KeyLifetime { get; init; }

        protected override UserDelegationKey RequestUserDelegationKey(DateTimeOffset expiresOn)
        {
            KeyRequests++;

            //Positional: the factory has two overloads with the same parameter names in a different order
            return BlobsModelFactory.UserDelegationKey(
                KeyObjectId,
                "00000000-0000-0000-0000-000000000001",
                DateTimeOffset.UtcNow,
                KeyLifetime is null ? expiresOn : DateTimeOffset.UtcNow.Add(KeyLifetime.Value),
                "b",
                "2025-01-05",
                Convert.ToBase64String(new byte[32]));
        }
    }
}
