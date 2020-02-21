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

        public ColumnCryptoMetaData CryptoMetadata
        {
            get
            {
                var handle = ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_CryptoMetadata);
                return handle == IntPtr.Zero ? null : new ColumnCryptoMetaData(handle);
            }
        }

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
        public bool IsStatsSet => ExceptionInfo.Return<bool>(_handle, ColumnChunkMetaData_Is_Stats_Set);
        public long NumValues => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Num_Values);
        public long TotalCompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Compressed_Size);
        public long TotalUncompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Uncompressed_Size);
        public Statistics Statistics => Statistics.Create(ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_Statistics));
        public PhysicalType Type => ExceptionInfo.Return<PhysicalType>(_handle, ColumnChunkMetaData_Type);
        
        [DllImport(ParquetDll.Name)]
        private static extern void ColumnChunkMetaData_Free(IntPtr columnChunkMetaData);
        
        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Compression(IntPtr columnChunkMetaData, out Compression compression);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_CryptoMetadata(IntPtr columnChunkMetaData, out IntPtr columnCryptoMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Encodings(IntPtr columnChunkMetaData, out IntPtr encodings);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Encodings_Count(IntPtr columnChunkMetaData, out ulong count);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_File_Offset(IntPtr columnChunkMetaData, out long fileOffset);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Is_Stats_Set(IntPtr columnChunkMetaData, [MarshalAs(UnmanagedType.I1)] out bool isStatsSet);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Num_Values(IntPtr columnChunkMetaData, out long numValues);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Statistics(IntPtr columnChunkMetaData, out IntPtr statistics);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Total_Compressed_Size(IntPtr columnChunkMetaData, out long totalCompressedSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Total_Uncompressed_Size(IntPtr columnChunkMetaData, out long totalUncompressedSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnChunkMetaData_Type(IntPtr columnChunkMetaData, out PhysicalType type);

        private readonly ParquetHandle _handle;
    }
}