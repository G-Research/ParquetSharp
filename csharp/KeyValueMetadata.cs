using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal sealed class KeyValueMetadata : IDisposable
    {
        public KeyValueMetadata() : this(MakeEmpty())
        {
        }

        public KeyValueMetadata(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, KeyValueMetadata_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        private long Size => ExceptionInfo.Return<long>(Handle, KeyValueMetadata_Size);

        public void SetData(IReadOnlyDictionary<string, string> keyValueMetadata)
        {
            using var byteBuffer = new ByteBuffer(1024);
            foreach (var entry in keyValueMetadata)
            {
                var keyPtr = StringUtil.ToCStringUtf8(entry.Key, byteBuffer);
                var valuePtr = StringUtil.ToCStringUtf8(entry.Value, byteBuffer);
                ExceptionInfo.Check(KeyValueMetadata_Append(Handle.IntPtr, keyPtr, valuePtr));
            }
        }

        public unsafe IReadOnlyDictionary<string, string> ToDictionary()
        {
            ExceptionInfo.Check(KeyValueMetadata_Get_Entries(Handle.IntPtr, out var keys, out var values));

            try
            {
                var size = checked((int) Size);
                var dict = new Dictionary<string, string>(size);

                for (int i = 0; i != size; ++i)
                {
                    var k = StringUtil.PtrToStringUtf8(((IntPtr*) keys)[i]);
                    var v = StringUtil.PtrToStringUtf8(((IntPtr*) values)[i]);

                    dict.Add(k, v);
                }

                return dict;
            }

            finally
            {
                KeyValueMetadata_Free_Entries(Handle.IntPtr, keys, values);
                GC.KeepAlive(Handle);
            }
        }

        private static IntPtr MakeEmpty()
        {
            ExceptionInfo.Check(KeyValueMetadata_MakeEmpty(out var handle));
            return handle;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_MakeEmpty(out IntPtr keyValueMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern void KeyValueMetadata_Free(IntPtr keyValueMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_Size(IntPtr keyValueMetadata, out long size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_Append(IntPtr keyValueMetadata, IntPtr key, IntPtr value);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_Get_Entries(IntPtr keyValueMetadata, out IntPtr keys, out IntPtr values);

        [DllImport(ParquetDll.Name)]
        private static extern void KeyValueMetadata_Free_Entries(IntPtr keyValueMetadata, IntPtr keys, IntPtr values);

        internal readonly ParquetHandle Handle;
    }
}
