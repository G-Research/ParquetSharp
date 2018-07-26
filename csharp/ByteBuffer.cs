using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Pool the ByteArray allocation into one buffer that we can pin, rather than many small byte[]. 
    /// </summary>
    internal sealed class ByteBuffer : IDisposable
    {
        public ByteBuffer(int capacity)
        {
            _initialCapacity = capacity;
        }

        public void Dispose()
        {
            _handle?.Free();
            _handle = null;
        }

        ~ByteBuffer()
        {
            Dispose();
        }

        public void Clear()
        {
            _size = 0;
        }

        public ByteArray Allocate(int length)
        {
            if (_buffer == null || _handle == null)
            {
                _buffer = new byte[_initialCapacity];
                _handle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            }

            if (_size + length > _buffer.Length)
            {
                Dispose();

                var newCapacity = Math.Max(_size + length, _buffer.Length * 2);
                var newBuffer = new byte[newCapacity];

                Array.Copy(_buffer, newBuffer, _size);

                _buffer = newBuffer;
                _handle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            }

            var offset = _size;
            _size += length;

            return new ByteArray(_handle.Value.AddrOfPinnedObject() + offset, length);
        }

        private readonly int _initialCapacity;
        private GCHandle? _handle;
        private byte[] _buffer;
        private int _size;
    }
}
