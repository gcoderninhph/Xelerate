using ProcessServer;

namespace Xelerate;

public readonly struct RegionMessage
{
    public readonly RegionMessageType Type;
    
    // Chỉ 1 trong 2 field này có giá trị tùy vào Type
    public readonly ProcessPayload? Payload;
    public readonly XelerateItem? Item;

    // Khởi tạo cho ProcessPayload
    public RegionMessage(ProcessPayload payload)
    {
        Type = RegionMessageType.ProcessPayload;
        Payload = payload;
        Item = null;
    }

    // Khởi tạo cho ProcessItem (Done hoặc Delete)
    public RegionMessage(RegionMessageType type, XelerateItem item)
    {
        Type = type;
        Payload = null;
        Item = item;
    }
}