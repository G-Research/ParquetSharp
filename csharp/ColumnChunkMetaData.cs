using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents the metadata for a column chunk.
    /// </summary>
    /// <remarks>
    /// Provides access to the metadata for a column chunk and manages the associated native resources.
    /// Because this class is a wrapper around C++ objects, it implements <see cref="IDisposable"/> to release resources predictably.
    /// Make sure to call <see cref="Dispose"/> or use a `using` statement for proper cleanup.
    /// </remarks>
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

        /// <summary>
        /// Get the compression codec used for the column chunk.
        /// </summary>
        /// <value>A <see cref="ParquetSharp.Compression"/> value representing the compression codec used for the column chunk.</value>
        public Compression Compression => ExceptionInfo.Return<Compression>(_handle, ColumnChunkMetaData_Compression);

        /// <summary>
        /// Get the encryption metadata associated with the column.
        /// </summary>
        /// <value>A <see cref="ColumnCryptoMetaData"/> object representing the encryption metadata for the column, or <see langword="null"/> if no encryption metadata is available.</value>
        public ColumnCryptoMetaData? CryptoMetadata
        {
            get
            {
                var handle = ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_CryptoMetadata);
                return handle == IntPtr.Zero ? null : new ColumnCryptoMetaData(handle);
            }
        }

        /// <summary>
        /// Get the encodings used for the column chunk.
        /// </summary>
        /// <value>An array of <see cref="Encoding"/> values representing the encodings used for the column chunk.</value>
        public unsafe Encoding[] Encodings
        {
            get
            {
                var count = ExceptionInfo.Return<ulong>(_handle, ColumnChunkMetaData_Encodings_Count);
                var src = (Encoding*) ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_Encodings);
                var encodings = new Encoding[count];

                fixed (Encoding* dst = encodings)
                {
                    Buffer.MemoryCopy(src, dst, count * sizeof(Encoding), count * sizeof(Encoding));
                }

                return encodings;
            }
        }

        /// <summary>
        /// Get the offset of the column chunk in the file.
        /// </summary>
        public long FileOffset => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_File_Offset);
        /// <summary>
        /// Whether the column chunk statistics are present in the metadata.
        /// </summary>
        public bool IsStatsSet => ExceptionInfo.Return<bool>(_handle, ColumnChunkMetaData_Is_Stats_Set);
        /// <summary>
        /// Get the total number of values in the column chunk.
        /// </summary>
        public long NumValues => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Num_Values);
        /// <summary>
        /// Get the total compressed size of the column chunk in bytes.
        /// </summary>
        public long TotalCompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Compressed_Size);
        /// <summary>
        /// Get the total uncompressed size of the column chunk in bytes.
        /// </summary>
        public long TotalUncompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Uncompressed_Size);
        /// <summary>
        /// Get the statistics for the column chunk.
        /// </summary>
        /// <value>A <see cref="ParquetSharp.Statistics"/> object representing the statistics for the column chunk, or <see langword="null"/> if no statistics are available.</value>
        public Statistics? Statistics => Statistics.Create(ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_Statistics));
        /// <summary>
        /// Get the physical type of the column chunk.
        /// </summary>
        /// <value>A <see cref="PhysicalType"/> value representing the physical type of the column chunk.</value>
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
