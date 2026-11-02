using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Points to a piece of contiguous memory.
    /// </summary>
    public class Buffer : IDisposable
    {
        public Buffer(IntPtr data, long size)
            : this(Make(data, size))
        {
        }

        internal Buffer(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, Buffer_Free);
        }

        internal Buffer(ParquetHandle handle)
        {
            Handle = handle;
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public long Capacity => ExceptionInfo.Return<long>(Handle, Buffer_Capacity);
        public IntPtr Data => ExceptionInfo.Return<IntPtr>(Handle, Buffer_Data);
        public IntPtr MutableData => ExceptionInfo.Return<IntPtr>(Handle, Buffer_MutableData);
        public long Size => ExceptionInfo.Return<long>(Handle, Buffer_Size);

        public byte[] ToArray()
        {
            var array = new byte[Size];
            Marshal.Copy(Data, array, 0, array.Length);
            return array;
        }

        private static IntPtr Make(IntPtr data, long size)
        {
            ExceptionInfo.Check(Buffer_MakeFromPointer(data, size, out var bufferHandle));
            return bufferHandle;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Buffer_MakeFromPointer(IntPtr data, long size, out IntPtr buffer);

        [DllImport(ParquetDll.Name)]
        private static extern void Buffer_Free(IntPtr buffer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Buffer_Capacity(IntPtr buffer, out long capacity);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Buffer_Data(IntPtr buffer, out IntPtr data);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Buffer_MutableData(IntPtr buffer, out IntPtr data);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Buffer_Size(IntPtr buffer, out long size);

        internal readonly ParquetHandle Handle;
    }
}
