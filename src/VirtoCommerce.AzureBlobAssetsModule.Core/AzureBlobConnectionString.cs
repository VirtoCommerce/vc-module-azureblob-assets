using System;
using System.Collections.Concurrent;
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

    // One credential per principal: a credential caches its access token, so a client created per operation
    // does not request a new token every time.
    private static readonly ConcurrentDictionary<(string TenantId, string ClientId, string ClientSecret), ClientSecretCredential> _credentials = new();

    public static BlobServiceClient CreateServiceClient(string connectionString)
    {
        var servicePrincipal = ParseServicePrincipal(connectionString);
        if (servicePrincipal is null)
        {
            return new BlobServiceClient(connectionString);
        }

        var credential = _credentials.GetOrAdd(
            (servicePrincipal.TenantId, servicePrincipal.ClientId, servicePrincipal.ClientSecret),
            key => new ClientSecretCredential(key.TenantId, key.ClientId, key.ClientSecret));

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

        // The message names the missing keys only: the connection string holds a secret and is never repeated.
        var blobEndpoint = GetRequiredValue(builder, BlobEndpointKey);
        if (!Uri.TryCreate(blobEndpoint, UriKind.Absolute, out var blobEndpointUri))
        {
            throw new ArgumentException($"The {BlobEndpointKey} of a service principal connection string is not an absolute url.", nameof(connectionString));
        }

        return new ServicePrincipal(
            blobEndpointUri,
            GetRequiredValue(builder, TenantIdKey),
            GetRequiredValue(builder, ClientIdKey),
            GetRequiredValue(builder, ClientSecretKey));
    }

    private static string GetRequiredValue(DbConnectionStringBuilder builder, string key)
    {
        return builder.TryGetValue(key, out var value) && value is string text && !string.IsNullOrWhiteSpace(text)
            ? text
            : throw new ArgumentException(
                $"A service principal connection string requires {BlobEndpointKey}, {TenantIdKey}, {ClientIdKey} and {ClientSecretKey}; {key} is missing.",
                "connectionString");
    }

    private sealed record ServicePrincipal(Uri BlobEndpoint, string TenantId, string ClientId, string ClientSecret);
}
