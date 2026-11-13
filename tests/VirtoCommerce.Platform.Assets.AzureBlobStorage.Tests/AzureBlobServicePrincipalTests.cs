using System;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
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

        //The key is requested for six days, so it covers any url that expires within them
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
    public void GenerateSasUrl_ServicePrincipal_RequestsTheKeyForSixDays()
    {
        var provider = new FakeKeyProvider();

        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromHours(1));

        //A day short of the seven-day limit, so a clock running ahead of the storage account's is not refused
        Assert.InRange(provider.LastRequestedExpiry, DateTimeOffset.UtcNow.AddDays(6).AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(6).AddMinutes(1));
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalLongerThanTheKey_ExpiresWithTheKey()
    {
        var provider = new FakeKeyProvider();

        var signedUrl = provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(6));

        Assert.Equal(provider.LastRequestedExpiry, new BlobUriBuilder(new Uri(signedUrl)).Sas.ExpiresOn, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalLongUrlsAndAKeyUnderADayOld_RequestOneKey()
    {
        var provider = new FakeKeyProvider { KeyLifetime = TimeSpan.FromDays(5.5) };

        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromHours(1));
        var signedUrl = provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(6));

        Assert.Equal(1, provider.KeyRequests);
        Assert.InRange(new BlobUriBuilder(new Uri(signedUrl)).Sas.ExpiresOn, DateTimeOffset.UtcNow.AddDays(5.5).AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(5.5).AddMinutes(1));
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalLongUrlAndAKeyOverADayOld_RequestsANewKey()
    {
        var provider = new FakeKeyProvider { KeyLifetime = TimeSpan.FromDays(4) };

        provider.GenerateSasUrl(BlobUrl, TimeSpan.FromHours(1));
        provider.KeyLifetime = null;
        var signedUrl = provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(6));

        Assert.Equal(2, provider.KeyRequests);
        Assert.Equal(provider.LastRequestedExpiry, new BlobUriBuilder(new Uri(signedUrl)).Sas.ExpiresOn, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalUrlsSignedAtOnceOnAColdProvider_RequestOneKey()
    {
        var provider = new FakeKeyProvider { RequestDuration = TimeSpan.FromMilliseconds(200) };

        Parallel.For(0, 8, new ParallelOptions { MaxDegreeOfParallelism = 8 }, _ => provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(1)));

        Assert.Equal(1, provider.KeyRequests);
    }

    [Fact]
    public async Task GetTagsAsync_BlobWithoutTags_DoesNotAskForThem()
    {
        var blob = new Mock<BlobBaseClient>();

        var tags = await AzureBlobProvider.GetTagsAsync(blob.Object, BlobsModelFactory.BlobProperties(tagCount: 0));

        Assert.Null(tags);
        blob.Verify(x => x.GetTagsAsync(It.IsAny<BlobRequestConditions>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task GetTagsAsync_RoleWithoutTagsRead_ReturnsNoTagsInsteadOfFailing()
    {
        //Storage Blob Data Reader and Contributor read a blob, but Get Blob Tags answers them 403
        var blob = new Mock<BlobBaseClient>();
        blob.Setup(x => x.GetTagsAsync(It.IsAny<BlobRequestConditions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new RequestFailedException(403, "This request is not authorized to perform this operation using this permission.", "AuthorizationPermissionMismatch", null));

        var tags = await AzureBlobProvider.GetTagsAsync(blob.Object, BlobsModelFactory.BlobProperties(tagCount: 2));

        Assert.Null(tags);
    }

    [Fact]
    public async Task GetTagsAsync_OtherFailure_IsNotSwallowed()
    {
        var blob = new Mock<BlobBaseClient>();
        blob.Setup(x => x.GetTagsAsync(It.IsAny<BlobRequestConditions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new RequestFailedException(503, "Server busy", "ServerBusy", null));

        await Assert.ThrowsAsync<RequestFailedException>(() => AzureBlobProvider.GetTagsAsync(blob.Object, BlobsModelFactory.BlobProperties(tagCount: 2)));
    }

    [Fact]
    public void GenerateSasUrl_ServicePrincipalLongerThanSixDays_Throws()
    {
        var provider = new FakeKeyProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() => provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(6).Add(TimeSpan.FromSeconds(1))));
        Assert.Equal(0, provider.KeyRequests);
    }

    [Fact]
    public void GenerateSasUrl_AccountKeyLongerThanSixDays_SignsAsBefore()
    {
        var provider = CreateProvider(_accountKeyConnectionString);

        var signedUrl = provider.GenerateSasUrl(BlobUrl, TimeSpan.FromDays(30));

        Assert.Null(new BlobUriBuilder(new Uri(signedUrl)).Sas.KeyObjectId);
    }

    [Theory]
    [InlineData("https://myaccount.blob.core.windows.net/mycontainer/folder/a%2520b.csv", "folder/a%20b.csv")]
    [InlineData("https://myaccount.blob.core.windows.net/mycontainer/folder/a+b.csv", "folder/a+b.csv")]
    [InlineData("https://myaccount.blob.core.windows.net/mycontainer/folder/a%2Bb.csv", "folder/a+b.csv")]
    [InlineData("https://myaccount.blob.core.windows.net/mycontainer/folder/a%20b.csv", "folder/a b.csv")]
    [InlineData("/mycontainer/folder/a%20b.csv", "folder/a b.csv")]
    [InlineData("mycontainer/folder/a+b.csv", "folder/a+b.csv")]
    public void GenerateSasUrl_EscapedBlobName_SignsTheSameBlob(string blobUrl, string blobName)
    {
        var accountKeyUrl = new BlobUriBuilder(new Uri(CreateProvider(_accountKeyConnectionString).GenerateSasUrl(blobUrl, TimeSpan.FromDays(1))));
        var servicePrincipalUrl = new BlobUriBuilder(new Uri(new FakeKeyProvider().GenerateSasUrl(blobUrl, TimeSpan.FromDays(1))));

        Assert.Equal(("mycontainer", blobName), (accountKeyUrl.BlobContainerName, accountKeyUrl.BlobName));
        Assert.Equal(("mycontainer", blobName), (servicePrincipalUrl.BlobContainerName, servicePrincipalUrl.BlobName));
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
        private int _keyRequests;

        public int KeyRequests => _keyRequests;

        public DateTimeOffset LastRequestedExpiry { get; private set; }

        // Null: the key lasts as long as requested. A shorter one stands for a key requested days ago
        public TimeSpan? KeyLifetime { get; set; }

        // How long a key request takes, so callers arriving together overlap it
        public TimeSpan RequestDuration { get; set; }

        protected override UserDelegationKey RequestUserDelegationKey(DateTimeOffset expiresOn)
        {
            Interlocked.Increment(ref _keyRequests);
            Thread.Sleep(RequestDuration);
            LastRequestedExpiry = expiresOn;

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
