using Confluent.Kafka;
using System;
using System.Buffers;

namespace Xelerate;

public class PooledBytesDeserializer : IDeserializer<PooledMessage>
{
    public PooledMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.Length == 0)
            return new PooledMessage(null, 0);

        // 1. Mượn mảng từ Pool
        var rentedArray = ArrayPool<byte>.Shared.Rent(data.Length);
        
        // 2. Copy dữ liệu từ C-core an toàn sang mảng mượn
        data.CopyTo(rentedArray);

        // 3. Đóng gói lại
        return new PooledMessage(rentedArray, data.Length);
    }
}