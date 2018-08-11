using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public sealed class ColumnChunkMetaData : IDisposable
    {
        internal ColumnChunkMetaData(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, ColumnChunkMetaData_Free);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        public Compression Compression => ExceptionInfo.Return<Compression>(_handle, ColumnChunkMetaData_Compression);
        public unsafe Encoding[] Encodings
        {
            get
            {
                var count = ExceptionInfo.Return<ulong>(_handle, ColumnChunkMetaData_Encodings_Count);
                var src = (Encoding*) ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_Encodings);
                var encodings = new Encoding[count];

                fixed (Encoding* dst = encodings)
                {
                    Buffer.MemoryCopy(src, dst, count*sizeof(Encoding), count*sizeof(Encoding));
                }

                return encodings;
            }
        }
        public long FileOffset => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_File_Offset);
        public long NumValues => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Num_Values);
        public long TotalCompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Compressed_Size);
        public long TotalUncompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Uncompressed_Size);
        public ParquetType Type => ExceptionInfo.Return<ParquetType>(_handle, ColumnChunkMetaData_Type);


        [DllImport(ParquetDll.Name)]
        private static extern void ColumnChunkMetaData_Free(IntPtr columnChunkMetaData);
        
        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Compression(IntPtr columnChunkMetaData, out Compression compresison);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Encodings(IntPtr columnChunkMetaData, out IntPtr encodings);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Encodings_Count(IntPtr columnChunkMetaData, out ulong count);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_File_Offset(IntPtr columnChunkMetaData, out long fileOffset);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Num_Values(IntPtr columnChunkMetaData, out long numValues);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Total_Compressed_Size(IntPtr columnChunkMetaData, out long totalCompressedSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Total_Uncompressed_Size(IntPtr columnChunkMetaData, out long totalUncompressedSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Type(IntPtr columnChunkMetaData, out ParquetType type);

        private readonly ParquetHandle _handle;
    }
}