using ProcessServer;

namespace Xelerate;

public class XelerateItem
{
    public string UnitType { get; set; } = string.Empty;
    public long UnitId { get; set; }
    public long TimeTargetMs { get; set; }
    public int Version { get; set; }
    public bool Status { get; set; }
    public byte[]? OriginalData { get; set; } // Dữ liệu gốc
    public int DataLength { get; set; }
    public ProcessType Type { get; set; }
}