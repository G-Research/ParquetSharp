using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents the metadata for a column chunk.
    /// </summary>
    /// <remarks>
    /// This class provides access to the metadata for a column chunk and manages the associated native resources.
    /// Because this class is a wrapper around C++ objects, it implements <see cref="IDisposable"/> to release resources predictably.
    /// Make sure to call <see cref="Dispose"/> or use a `using` statement for proper cleanup.
    /// </remarks>
    public sealed class ColumnChunkMetaData : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnChunkMetaData"/> class with the specified native handle.
        /// </summary>
        /// <param name="handle">A pointer to the native Parquet column chunk metadata object.</param>
        /// <remarks>
        /// This constructor is intended for internal use. The <paramref name="handle"/> should be a valid pointer to avoid runtime errors.
        /// </remarks>
        internal ColumnChunkMetaData(IntPtr handle)
        {
            _handle = new ParquetHandle(handle, ColumnChunkMetaData_Free);
        }

        /// <summary>
        /// Releases resources used by the current instance of the <see cref="ColumnChunkMetaData"/> class.
        /// </summary>
        /// <remarks>
        /// This method should be called to release unmanaged resources. Alternatively, use a `using` statement to ensure proper disposal.
        /// </remarks>
        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Gets the compression codec used for the column chunk.
        /// </summary>
        /// <value>A <see cref="ParquetSharp.Compression"/> value representing the compression codec used for the column chunk.</value>
        public Compression Compression => ExceptionInfo.Return<Compression>(_handle, ColumnChunkMetaData_Compression);

        /// <summary>
        /// Gets the encryption metadata associated with the column.
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
        /// Gets the encodings used for the column chunk.
        /// </summary>
        /// <value>An array of <see cref="Encoding"/> values representing the encodings used for the column chunk.</value>
        /// <exception cref="ParquetException">Thrown if the encodings cannot be retrieved.</exception>
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
        /// Gets the offset of the column chunk in the file.
        /// </summary>
        public long FileOffset => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_File_Offset);
        /// <summary>
        /// Indicates whether the column chunk statistics are present in the metadata.
        /// </summary>
        public bool IsStatsSet => ExceptionInfo.Return<bool>(_handle, ColumnChunkMetaData_Is_Stats_Set);
        /// <summary>
        /// Gets the total number of values in the column chunk.
        /// </summary>
        public long NumValues => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Num_Values);
        /// <summary>
        /// Gets the total compressed size of the column chunk in bytes.
        /// </summary>
        public long TotalCompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Compressed_Size);
        /// <summary>
        /// Gets the total uncompressed size of the column chunk in bytes.
        /// </summary>
        public long TotalUncompressedSize => ExceptionInfo.Return<long>(_handle, ColumnChunkMetaData_Total_Uncompressed_Size);
        /// <summary>
        /// Gets the statistics for the column chunk.
        /// </summary>
        /// <value>A <see cref="ParquetSharp.Statistics"/> object representing the statistics for the column chunk, or <see langword="null"/> if no statistics are available.</value>
        public Statistics? Statistics => Statistics.Create(ExceptionInfo.Return<IntPtr>(_handle, ColumnChunkMetaData_Statistics));
        /// <summary>
        /// Gets the physical type of the column chunk.
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
