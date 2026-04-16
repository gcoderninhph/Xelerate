using System.Buffers;
using Microsoft.Extensions.ObjectPool;

namespace Xelerate;

public class XelerateItemPooledObjectPolicy : PooledObjectPolicy<XelerateItem>
{
    public override XelerateItem Create()
    {
        return new XelerateItem();
    }

    public override bool Return(XelerateItem obj)
    {
        if (obj.OriginalData != null)
        {
            ArrayPool<byte>.Shared.Return(obj.OriginalData);
            obj.OriginalData = null;
        }
        obj.DataLength = 0;
        obj.UnitType = string.Empty;
        obj.Version = 0;
        obj.UnitId = 0;
        obj.TimeTargetMs = 0;
        obj.Status = false;

        return true;
    }
}