using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal sealed class KeyValueMetadata : IDisposable
    {
        public KeyValueMetadata(IReadOnlyDictionary<string, string> keyValueMetadata)
            : this(Make(keyValueMetadata))
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

        public long Size => ExceptionInfo.Return<long>(Handle, KeyValueMetadata_Size);

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

        private static unsafe IntPtr Make(IReadOnlyDictionary<string, string> keyValueMetadata)
        {
            using var byteBuffer = new ByteBuffer(1024);
            var keys = new IntPtr[keyValueMetadata.Count];
            var values = new IntPtr[keyValueMetadata.Count];
            var i = 0;

            foreach (var entry in keyValueMetadata)
            {
                keys[i] = StringUtil.ToCStringUtf8(entry.Key, byteBuffer);
                values[i] = StringUtil.ToCStringUtf8(entry.Value, byteBuffer);

                ++i;
            }

            fixed (IntPtr* pKeys = keys)
            fixed (IntPtr* pValues = values)
            {
                ExceptionInfo.Check(KeyValueMetadata_Make(values.Length, new IntPtr(pKeys), new IntPtr(pValues), out var handle));
                return handle;
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_Make(long size, IntPtr keys, IntPtr values, out IntPtr keyValueMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern void KeyValueMetadata_Free(IntPtr keyValueMetadata);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_Size(IntPtr keyValueMetadata, out long size);
        
        [DllImport(ParquetDll.Name)]
        private static extern IntPtr KeyValueMetadata_Get_Entries(IntPtr keyValueMetadata, out IntPtr keys, out IntPtr values);

        [DllImport(ParquetDll.Name)]
        private static extern void KeyValueMetadata_Free_Entries(IntPtr keyValueMetadata, IntPtr keys, IntPtr values);

        internal readonly ParquetHandle Handle;
    }
}
