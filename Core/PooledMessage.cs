using System;
using System.Buffers;

namespace Xelerate;

public struct PooledMessage(byte[]? array, int length) : IDisposable
{
    private byte[]? _rentedArray = array;
    public int Length { get; } = length;

    // Trả ra Span chính xác với dữ liệu thực, cắt bỏ phần byte thừa của ArrayPool
    public Span<byte> Span => _rentedArray != null ? _rentedArray.AsSpan(0, Length) : Span<byte>.Empty;

    public void Dispose()
    {
        if (_rentedArray != null)
        {
            ArrayPool<byte>.Shared.Return(_rentedArray);
            _rentedArray = null;
        }
    }
}