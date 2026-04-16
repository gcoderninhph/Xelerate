using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

#nullable enable

/// <summary>
/// Hierarchical Time Wheel — xử lý 1M+ objects hiệu quả.
///
/// Kiến trúc 3 tầng wheel:
///   Wheel 0 (ms)  : 256 slots × 10ms  = 2.56s
///   Wheel 1 (sec) : 64  slots × 2.56s = ~2.73 phút
///   Wheel 2 (min) : 64  slots × 2.73m = ~2.9 giờ
///
/// Độ phức tạp:
///   Add    : O(1)
///   Update : O(1)
///   Remove : O(1)
///   Expire : O(1) per object
/// </summary>
public class TimedSortedSet<TKey, TValue> : IDisposable
    where TKey : notnull
{
    // ───────────────────────────────────────────────
    //  Constants
    // ───────────────────────────────────────────────

    private const int TICK_MS = 10;

    private const int W0_SIZE = 1 << 8; // 256
    private const int W1_SIZE = 1 << 6; // 64
    private const int W2_SIZE = 1 << 6; // 64

    private const int W0_MASK = W0_SIZE - 1;
    private const int W1_MASK = W1_SIZE - 1;
    private const int W2_MASK = W2_SIZE - 1;

    private const long W0_RANGE = W0_SIZE;
    private const long W1_RANGE = W0_RANGE * W1_SIZE;
    private const long W2_RANGE = W1_RANGE * W2_SIZE;

    // ───────────────────────────────────────────────
    //  Entry
    // ───────────────────────────────────────────────

    private sealed class TimedEntry
    {
        public TKey Key { get; }
        public TValue Value { get; }
        public long ExpireTime { get; set; }

        public TimedEntry(TKey key, TValue value, long expireTime)
        {
            Key = key;
            Value = value;
            ExpireTime = expireTime;
        }
    }

    // ───────────────────────────────────────────────
    //  Wheel storage
    // ───────────────────────────────────────────────

    private readonly LinkedList<TimedEntry>[] _wheel0 = CreateWheel(W0_SIZE);
    private readonly LinkedList<TimedEntry>[] _wheel1 = CreateWheel(W1_SIZE);
    private readonly LinkedList<TimedEntry>[] _wheel2 = CreateWheel(W2_SIZE);

    private static LinkedList<TimedEntry>[] CreateWheel(int size)
    {
        var w = new LinkedList<TimedEntry>[size];
        for (int i = 0; i < size; i++) w[i] = new LinkedList<TimedEntry>();
        return w;
    }

    private readonly Dictionary<TKey, LinkedListNode<TimedEntry>> _lookup = new();
    private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);

    private long _currentTick;
    private readonly long _startMs;
    private bool _disposed;

    // ───────────────────────────────────────────────
    //  Static shared timer
    // ───────────────────────────────────────────────

    private static readonly List<WeakReference<TimedSortedSet<TKey, TValue>>> s_instances = new();
    private static readonly object s_instanceLock = new();
    private static readonly Thread s_globalTimer;

    static TimedSortedSet()
    {
        s_globalTimer = new Thread(GlobalTimerLoop)
        {
            IsBackground = true,
            Name = $"TimeWheel<{typeof(TKey).Name},{typeof(TValue).Name}>"
        };
        s_globalTimer.Start();
    }

    private static void GlobalTimerLoop()
    {
        while (true)
        {
            Thread.Sleep(TICK_MS);

            List<WeakReference<TimedSortedSet<TKey, TValue>>> snapshot;
            lock (s_instanceLock)
            {
                s_instances.RemoveAll(wr => !wr.TryGetTarget(out _));
                snapshot = new(s_instances);
            }

            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var wr in snapshot)
                if (wr.TryGetTarget(out var inst))
                    inst.Advance(now);
        }
    }

    // ───────────────────────────────────────────────
    //  Event
    // ───────────────────────────────────────────────

    public event Action<IReadOnlyList<(TKey Key, TValue Value)>>? OnExpired;
    public event Action<IReadOnlyList<(TKey Key, TValue Value)>>? OnRemoved; // ← thêm mới

    // ───────────────────────────────────────────────
    //  Constructor / Dispose
    // ───────────────────────────────────────────────

    public TimedSortedSet()
    {
        _startMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        lock (s_instanceLock)
            s_instances.Add(new WeakReference<TimedSortedSet<TKey, TValue>>(this));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _rwLock.Dispose();
    }

    // ───────────────────────────────────────────────
    //  Public API
    // ───────────────────────────────────────────────

    /// <summary>
    /// Thêm mới hoặc cập nhật expireTime nếu key đã tồn tại.
    /// </summary>
    /// <param name="expireTime">Unix milliseconds — DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + offset</param>
    public void AddOrUpdate(TKey key, TValue value, long expireTime)
    {
        _rwLock.EnterWriteLock();
        try
        {
            if (_lookup.TryGetValue(key, out var existing))
                existing.List!.Remove(existing);

            var entry = new TimedEntry(key, value, expireTime);
            _lookup[key] = InsertToWheel(entry);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public bool Remove(TKey key)
    {
        TValue removedValue;

        _rwLock.EnterWriteLock();
        try
        {
            if (!_lookup.TryGetValue(key, out var node)) return false;

            removedValue = node.Value.Value;
            node.List!.Remove(node);
            _lookup.Remove(key);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }

        // Kích hoạt event SAU khi release lock
        OnRemoved?.Invoke(new List<(TKey, TValue)> { (key, removedValue) }.AsReadOnly());
        return true;
    }

    public int Count
    {
        get
        {
            _rwLock.EnterReadLock();
            try
            {
                return _lookup.Count;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
    }

    // ───────────────────────────────────────────────
    //  Wheel logic
    // ───────────────────────────────────────────────

    private long ToRelativeTick(long unixMs)
    {
        long diff = unixMs - _startMs;
        if (diff <= 0) return 0;
        return (diff + TICK_MS - 1) / TICK_MS; // ceiling
    }

    private LinkedListNode<TimedEntry> InsertToWheel(TimedEntry entry)
    {
        long targetTick = ToRelativeTick(entry.ExpireTime);
        long diff = targetTick - _currentTick;

        LinkedList<TimedEntry> slot;

        if (diff < W0_RANGE)
            slot = _wheel0[targetTick & W0_MASK];
        else if (diff < W1_RANGE)
            slot = _wheel1[(targetTick / W0_RANGE) & W1_MASK];
        else if (diff < W2_RANGE)
            slot = _wheel2[(targetTick / W1_RANGE) & W2_MASK];
        else
            slot = _wheel2[W2_SIZE - 1];

        return slot.AddLast(entry);
    }

    private void Advance(long nowTicks)
    {
        if (_disposed) return;

        long targetTick = ToRelativeTick(nowTicks);
        List<(TKey, TValue)>? expired = null;

        _rwLock.EnterWriteLock();
        try
        {
            while (_currentTick <= targetTick)
            {
                int slot0 = (int)(_currentTick & W0_MASK);

                if (slot0 == 0)
                {
                    int slot1 = (int)((_currentTick / W0_RANGE) & W1_MASK);
                    if (slot1 == 0)
                        Cascade(_wheel2[(int)((_currentTick / W1_RANGE) & W2_MASK)]);
                    Cascade(_wheel1[slot1]);
                }

                var bucket = _wheel0[slot0];
                var node = bucket.First;
                while (node != null)
                {
                    var next = node.Next;
                    _lookup.Remove(node.Value.Key);
                    expired ??= new();
                    expired.Add((node.Value.Key, node.Value.Value));
                    bucket.Remove(node);
                    node = next;
                }

                _currentTick++;
            }
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }

        try
        {
            if (expired is { Count: > 0 })
                OnExpired?.Invoke(expired.AsReadOnly());
        }
        catch (Exception e)
        {
            //
        }
    }

    private void Cascade(LinkedList<TimedEntry> bucket)
    {
        var node = bucket.First;
        while (node != null)
        {
            var next = node.Next;
            bucket.Remove(node);
            InsertToWheel(node.Value);
            node = next;
        }
    }
}