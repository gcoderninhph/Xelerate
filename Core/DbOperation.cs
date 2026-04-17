namespace Xelerate;

public enum DbOpType { Upsert, Delete }

public struct DbOperation
{
    public DbOpType OpType;
    public long RegionId;
    public string UnitType;
    public long UnitId;
    public long TimeTargetMs;
    // Sử dụng mảng byte độc lập để tránh xung đột vùng nhớ ArrayPool với Region
    public byte[]? Data; 
}