using Microsoft.Extensions.ObjectPool;
using ProcessServer;

namespace Xelerate;

public class XeleratePayloadPooledObjectPolicy : PooledObjectPolicy<ProcessPayload>
{
    public override ProcessPayload Create()
    {
        return new ProcessPayload();
    }

    public override bool Return(ProcessPayload obj)
    {
        obj.Clear();
        return true;
    }
}