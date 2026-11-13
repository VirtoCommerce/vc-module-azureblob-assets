using System;
using System.Data.Common;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace VirtoCommerce.AzureBlobAssetsModule.Core;

/// <summary>
/// Creates a <see cref="BlobServiceClient"/> from a connection string. Besides the forms Azure Storage accepts (an
/// account key, a shared access signature), a service principal of Microsoft Entra ID is accepted:
/// <c>BlobEndpoint=https://{account}.blob.core.windows.net;TenantId={tenant id};ClientId={client id};ClientSecret={client secret}</c>.
/// What the principal may do is decided by its role on the storage account, not by the connection string.
/// </summary>
public static class AzureBlobConnectionString
{
    private const string BlobEndpointKey = "BlobEndpoint";
    private const string TenantIdKey = "TenantId";
    private const string ClientIdKey = "ClientId";
    private const string ClientSecretKey = "ClientSecret";

    /// <remarks>
    /// A service principal gets a credential of its own, which caches and refreshes its access token. So keep the
    /// client rather than create one per operation: a new client requests a new token on its first call.
    /// </remarks>
    public static BlobServiceClient CreateServiceClient(string connectionString)
    {
        return CreateServiceClient(connectionString, out _);
    }

    internal static BlobServiceClient CreateServiceClient(string connectionString, out bool isServicePrincipal)
    {
        var servicePrincipal = ParseServicePrincipal(connectionString);
        isServicePrincipal = servicePrincipal is not null;
        if (servicePrincipal is null)
        {
            return new BlobServiceClient(connectionString);
        }

        var credential = new ClientSecretCredential(servicePrincipal.TenantId, servicePrincipal.ClientId, servicePrincipal.ClientSecret);

        return new BlobServiceClient(servicePrincipal.BlobEndpoint, credential);
    }

    /// <summary>
    /// True when the connection string names a service principal rather than an account key or a shared access signature.
    /// </summary>
    public static bool IsServicePrincipal(string connectionString)
    {
        return ParseServicePrincipal(connectionString) is not null;
    }

    private static ServicePrincipal ParseServicePrincipal(string connectionString)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
        if (!builder.ContainsKey(TenantIdKey) && !builder.ContainsKey(ClientIdKey) && !builder.ContainsKey(ClientSecretKey))
        {
            return null;
        }

        // The messages name a key only: the connection string holds a secret and is never repeated.
        var blobEndpoint = GetValue(builder, BlobEndpointKey) ?? throw MissingKey(BlobEndpointKey, nameof(connectionString));
        if (!Uri.TryCreate(blobEndpoint, UriKind.Absolute, out var blobEndpointUri))
        {
            throw new ArgumentException($"The {BlobEndpointKey} of a service principal connection string is not an absolute url.", nameof(connectionString));
        }

        return new ServicePrincipal(
            blobEndpointUri,
            GetValue(builder, TenantIdKey) ?? throw MissingKey(TenantIdKey, nameof(connectionString)),
            GetValue(builder, ClientIdKey) ?? throw MissingKey(ClientIdKey, nameof(connectionString)),
            GetValue(builder, ClientSecretKey) ?? throw MissingKey(ClientSecretKey, nameof(connectionString)));
    }

    private static string GetValue(DbConnectionStringBuilder builder, string key)
    {
        return builder.TryGetValue(key, out var value) && value is string text && !string.IsNullOrWhiteSpace(text)
            ? text
            : null;
    }

    private static ArgumentException MissingKey(string key, string paramName)
    {
        return new ArgumentException(
            $"A service principal connection string requires {BlobEndpointKey}, {TenantIdKey}, {ClientIdKey} and {ClientSecretKey}; {key} is missing.",
            paramName);
    }

    // A class, not a record: a record's generated ToString would print the secret to whatever formats it
    private sealed class ServicePrincipal(Uri blobEndpoint, string tenantId, string clientId, string clientSecret)
    {
        public Uri BlobEndpoint { get; } = blobEndpoint;

        public string TenantId { get; } = tenantId;

        public string ClientId { get; } = clientId;

        public string ClientSecret { get; } = clientSecret;
    }
}
