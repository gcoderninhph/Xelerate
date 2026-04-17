using Google.Protobuf;
using ProcessServer;

namespace Xelerate;

public static class XeleratePayloadExtension
{
    public static void Add(this ProcessPayload processPayload, XelerateItem item)
    {
        processPayload.UnitIds.Add(item.UnitId);
        processPayload.TimeTargetMs.Add(item.TimeTargetMs);
        processPayload.Type.Add(item.Type);

        // processPayload.DataList.Add(UnsafeByteOperations.UnsafeWrap(item.OriginalData.AsMemory(item.DataLength)));
        processPayload.DataList.Add(
            item.OriginalData is { Length: > 0 }
                ? ByteString.CopyFrom(item.OriginalData, 0, item.DataLength)
                : ByteString.Empty);
    }

    public static void Clear(this ProcessPayload obj)
    {
        obj.UnitType = string.Empty;
        obj.RegionId = 0;

        obj.UnitIds.Clear();
        obj.DataList.Clear();
        obj.TimeTargetMs.Clear();
        obj.Type.Clear();
    }
}