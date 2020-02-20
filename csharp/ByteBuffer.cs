using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Pool the ByteArray allocations into few buffers that we can pin, rather than many small byte[].
    /// 
    /// This allows us to more efficiently pass byte-arrays to Parquet native API without having
    /// a pinning handle per byte[] (and indirectly, strings).
    /// </summary>
    public sealed class ByteBuffer : IDisposable
    {
        public ByteBuffer(int blockSize)
        {
            _blockSize = blockSize;
            _blocks = new List<Block>();
        }

        public void Dispose()
        {
            Clear();

            GC.SuppressFinalize(this);
        }

        ~ByteBuffer()
        {
            Clear();
        }

        public void Clear()
        {
            foreach (var block in _blocks)
            {
                block.Dispose();
            }

            _blocks.Clear();
        }

        public ByteArray Allocate(int length)
        {
            if (_blocks.Count == 0 || _blocks[_blocks.Count - 1].Available < length)
            {
                _blocks.Add(new Block(GetNextCapacity(length)));
            }

            return _blocks[_blocks.Count - 1].Allocate(length);
        }

        private int GetNextCapacity(int length)
        {
            // Start at blockSize for the initial block, but allocate 50% on each new block.
            if (_blocks.Count == 0)
            {
                return Math.Max(length, _blockSize);
            }

            var lastCapacity = _blocks[_blocks.Count - 1].Capacity;
            var newCapacity = Math.Max(2, lastCapacity + lastCapacity / 2);

            return Math.Max(length, newCapacity);
        }

        private sealed class Block : IDisposable
        {
            public Block(int capacity)
            {
                _buffer = new byte[capacity];
                _handle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
                _size = 0;
            }

            public void Dispose()
            {
                _handle.Free();
            }

            public int Available => _buffer.Length - _size;
            public int Capacity => _buffer.Length;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ByteArray Allocate(int length)
            {
                var byteArray = new ByteArray(_handle.AddrOfPinnedObject() + _size, length);
                _size += length;
                return byteArray;
            }

            private readonly byte[] _buffer;
            private GCHandle _handle;
            private int _size;
        }

        private readonly int _blockSize;
        private readonly List<Block> _blocks;
    }
}
