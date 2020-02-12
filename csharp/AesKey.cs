using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Internal fixed size structure for easily moving AES keys to and from C++.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Size = 16)]
    internal unsafe struct AesKey
    {
        public AesKey(byte[] key)
        {
            if (key.Length != 16 && key.Length != 24 && key.Length != 32)
            {
                throw new ArgumentException("AES key can only be 128, 192, or 256-bit in length", nameof(key));
            }

            fixed (byte* srcBytes = key)
            {
                var src = (ulong*) srcBytes;

                _key[0] = src[0];
                _key[1] = src[1];
                _key[2] = key.Length > 16 ? src[2] : 0;
                _key[3] = key.Length > 24 ? src[3] : 0;
            }

            _size = (uint) key.Length;
        }

        public byte[] ToBytes()
        {
            if (_size != 16 && _size != 24 && _size != 32)
            {
                throw new ArgumentException("AES key can only be 128, 192, or 256-bit in length", nameof(_size));
            }

            var bytes = new byte[_size];

            fixed (byte* dstBytes = bytes)
            {
                var dst = (ulong*) dstBytes;

                dst[0] = _key[0];
                dst[1] = _key[1];
                if (_size > 16) dst[2] = _key[2];
                if (_size > 24) dst[3] = _key[3];
            }

            return bytes;
        }

        private fixed ulong _key[4];
        private readonly uint _size;
    }
}
