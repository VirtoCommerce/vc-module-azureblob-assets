using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.AssetsModule.Core.Services;
using VirtoCommerce.AzureBlobAssetsModule.Core;

namespace VirtoCommerce.AzureBlobAssetsModule.Tests;

/// <summary>
/// A blob service endpoint on the loopback interface that answers every request with one status and
/// <c>x-ms-error-code</c>, and records the request lines it was sent. Lets a test see what the provider does with a
/// storage error, and which paths it asks for, without a storage account or an emulator.
/// </summary>
internal sealed class StubBlobService : IDisposable
{
    // The well-known Azurite account key: the provider needs a key to build its client, the stub never checks it
    private const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stopping = new();
    private readonly int _status;
    private readonly string _errorCode;

    public StubBlobService(HttpStatusCode status, string errorCode)
    {
        _status = (int)status;
        _errorCode = errorCode;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = AcceptAsync();
    }

    public string ConnectionString =>
        $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={AccountKey};BlobEndpoint=http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/devstoreaccount1;";

    /// <summary>
    /// The request lines received, such as <c>HEAD /devstoreaccount1/c/a.csv HTTP/1.1</c>.
    /// </summary>
    public ConcurrentQueue<string> RequestLines { get; } = new();

    public AzureBlobProvider CreateProvider()
    {
        var fileExtensionService = new Mock<IFileExtensionService>();
        fileExtensionService
            .Setup(x => x.IsExtensionAllowedAsync(It.IsAny<string>()))
            .ReturnsAsync(true);

        return new AzureBlobProvider(Options.Create(new AzureBlobOptions { ConnectionString = ConnectionString }), fileExtensionService.Object, eventPublisher: null);
    }

    public void Dispose()
    {
        _stopping.Cancel();
        _listener.Stop();
        _stopping.Dispose();
    }

    private async Task AcceptAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stopping.Token);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                return;
            }

            _ = AnswerAsync(client);
        }
    }

    private async Task AnswerAsync(TcpClient client)
    {
        using (client)
        {
            var stream = client.GetStream();
            var request = await ReadHeadAsync(stream);
            var requestLineEnd = request.IndexOf("\r\n", StringComparison.Ordinal);
            if (requestLineEnd < 0)
            {
                return;
            }

            RequestLines.Enqueue(request[..requestLineEnd]);

            var response = $"HTTP/1.1 {_status} Stub\r\nx-ms-error-code: {_errorCode}\r\nx-ms-request-id: {Guid.NewGuid()}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            await stream.WriteAsync(Encoding.ASCII.GetBytes(response));
        }
    }

    // The provider's requests carry no body that matters here, so the head is all there is to read
    private static async Task<string> ReadHeadAsync(NetworkStream stream)
    {
        var head = new StringBuilder();
        var buffer = new byte[4096];

        while (!head.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
        {
            var read = await stream.ReadAsync(buffer);
            if (read == 0)
            {
                break;
            }

            head.Append(Encoding.ASCII.GetString(buffer, 0, read));
        }

        return head.ToString();
    }
}
