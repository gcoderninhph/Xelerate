using NATS.Client.Core;

namespace Xelerate;

public class XelerateServer(string natsUrl) : IAsyncDisposable
{
    private NatsConnection _nats = new(new NatsOpts { Url = natsUrl });
    private CancellationTokenSource _cts = new();
    private Dictionary<long, Region> _regions = new();

    public async Task StartAsync()
    {
        await _nats.ConnectAsync();

        var ct = _cts.Token;

        _ = Task.Run(async () =>
        {
            await foreach (var msg in _nats.SubscribeAsync<NatsMemoryOwner<byte>>("ProcessServer.Region.*",
                               cancellationToken: ct))
            {
                var subject = msg.Subject;
                var regionId = long.Parse(subject.Split('.')[2]);
                using var memoryOwner = msg.Data;

                if (!_regions.TryGetValue(regionId, out var region))
                {
                    region = new Region(regionId, _nats);
                    _regions.Add(regionId, region);
                }

                Span<byte> data = memoryOwner.Span;
                region.EnqueueEvent(data);
            }
        }, ct);
    }

    // THÊM HÀM NÀY ĐỂ DỌN DẸP
    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();
        await _nats.DisposeAsync();

        foreach (var region in _regions.Values)
        {
            await region.DisposeAsync();
        }
    }
}