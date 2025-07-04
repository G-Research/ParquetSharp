using System;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Configures options for writing Parquet files.
    /// </summary>
    public sealed class WriterProperties : IDisposable
    {
        /// <summary>
        /// Represents a column sorting specification.
        /// </summary>
        public readonly struct SortingColumn : IEquatable<SortingColumn>
        {
            /// <summary>
            /// Creates a new sorting column specification.
            /// </summary>
            /// <param name="columnIndex">The index of the column to sort by</param>
            /// <param name="isDescending">Whether to sort in descending order (true) or ascending order (false)</param>
            /// <param name="nullsFirst">Whether nulls should come first (true) or last (false)</param>
            public SortingColumn(int columnIndex, bool isDescending = false, bool nullsFirst = false)
            {
                ColumnIndex = columnIndex;
                IsDescending = isDescending;
                NullsFirst = nullsFirst;
            }

            /// <summary>
            /// The index of the column to sort by
            /// </summary>
            public int ColumnIndex { get; }

            /// <summary>
            /// Whether to sort in descending order (true) or ascending order (false)
            /// </summary>
            public bool IsDescending { get; }

            /// <summary>
            /// Whether nulls should come first (true) or last (false)
            /// </summary>
            public bool NullsFirst { get; }

            public bool Equals(SortingColumn other)
            {
                return ColumnIndex == other.ColumnIndex && 
                       IsDescending == other.IsDescending && 
                       NullsFirst == other.NullsFirst;
            }

            public override bool Equals(object? obj)
            {
                return obj is SortingColumn other && Equals(other);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(ColumnIndex, IsDescending, NullsFirst);
            }

            public static bool operator ==(SortingColumn left, SortingColumn right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(SortingColumn left, SortingColumn right)
            {
                return !left.Equals(right);
            }
        }

        /// <summary>
        /// Create a new <see cref="WriterProperties"/> with default values.
        /// </summary>
        /// <returns>A new <see cref="WriterProperties"/> object with default values.</returns>
        public static WriterProperties GetDefaultWriterProperties()
        {
            return new WriterProperties(ExceptionInfo.Return<IntPtr>(WriterProperties_Get_Default_Writer_Properties));
        }

        internal WriterProperties(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, WriterProperties_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        /// <summary>
        /// The entity that created the file.
        /// </summary>
        public string CreatedBy => ExceptionInfo.ReturnString(Handle, WriterProperties_Created_By, WriterProperties_Created_By_Free);
        /// <summary>
        /// The size of the data pages in bytes.
        /// </summary>
        public long DataPageSize => ExceptionInfo.Return<long>(Handle, WriterProperties_Data_Pagesize);
        /// <summary>
        /// The <see cref="ParquetSharp.Encoding"/> used for dictionary index encoding.
        /// </summary>
        public Encoding DictionaryIndexEncoding => ExceptionInfo.Return<Encoding>(Handle, WriterProperties_Dictionary_Index_Encoding);
        /// <summary>
        /// The <see cref="ParquetSharp.Encoding"/> used for dictionary page encoding.
        /// </summary>
        public Encoding DictionaryPageEncoding => ExceptionInfo.Return<Encoding>(Handle, WriterProperties_Dictionary_Page_Encoding);
        /// <summary>
        /// The maximum dictionary page size in bytes.
        /// </summary>
        public long DictionaryPagesizeLimit => ExceptionInfo.Return<long>(Handle, WriterProperties_Dictionary_Pagesize_Limit);
        /// <summary>
        /// The <see cref="ParquetSharp.FileEncryptionProperties"/> used for writing encrypted files.
        /// </summary>
        public FileEncryptionProperties FileEncryptionProperties => new FileEncryptionProperties(ExceptionInfo.Return<IntPtr>(Handle, WriterProperties_File_Encryption_Properties));
        /// <summary>
        /// The maximum number of rows in a row group.
        /// </summary>
        public long MaxRowGroupLength => ExceptionInfo.Return<long>(Handle, WriterProperties_Max_Row_Group_Length);
        /// <summary>
        /// The version of the Parquet format to write.
        /// </summary>
        public ParquetVersion Version => ExceptionInfo.Return<CppParquetVersion>(Handle, WriterProperties_Version).ToPublicEnum();
        /// <summary>
        /// The number of records to batch together when writing.
        /// </summary>
        public long WriteBatchSize => ExceptionInfo.Return<long>(Handle, WriterProperties_Write_Batch_Size);

        /// <summary>
        /// Whether writing the page index is enabled for any columns
        /// </summary>
        public bool WritePageIndex => ExceptionInfo.Return<bool>(Handle, WriterProperties_Page_Index_Enabled);

        /// <summary>
        /// Whether writing the page index is enabled for the specified column
        /// </summary>
        public bool WritePageIndexForPath(ColumnPath path)
        {
            return ExceptionInfo.Return<bool>(Handle, path.Handle, WriterProperties_Page_Index_Enabled_For_Path);
        }

        /// <summary>
        /// Get the <see cref="ParquetSharp.Compression"/> type used for the specified column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column.</param>
        /// <returns>The <see cref="ParquetSharp.Compression"/> type used for the specified column.</returns>
        public Compression Compression(ColumnPath path)
        {
            return ExceptionInfo.Return<Compression>(Handle, path.Handle, WriterProperties_Compression);
        }

        /// <summary>
        /// Get the compression level used for the specified column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column.</param>
        /// <returns>The compression level used for the specified column.</returns>
        public int CompressionLevel(ColumnPath path)
        {
            return ExceptionInfo.Return<int>(Handle, path.Handle, WriterProperties_Compression_Level);
        }

        /// <summary>
        /// Whether dictionary encoding is enabled for the specified column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column.</param>
        /// <returns>Whether dictionary encoding is enabled for the specified column.</returns>
        public bool DictionaryEnabled(ColumnPath path)
        {
            return ExceptionInfo.Return<bool>(Handle, path.Handle, WriterProperties_Dictionary_Enabled);
        }

        /// <summary>
        /// Get the <see cref="ParquetSharp.Encoding"/> used for the specified column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column.</param>
        /// <returns>The <see cref="ParquetSharp.Encoding"/> used for the specified column.</returns>
        public Encoding Encoding(ColumnPath path)
        {
            return ExceptionInfo.Return<Encoding>(Handle, path.Handle, WriterProperties_Encoding);
        }

        /// <summary>
        /// Whether statistics are enabled for the specified column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column.</param>
        /// <returns>Whether statistics are enabled for the specified column.</returns>
        public bool StatisticsEnabled(ColumnPath path)
        {
            return ExceptionInfo.Return<bool>(Handle, path.Handle, WriterProperties_Statistics_Enabled);
        }

        /// <summary>
        /// The maximum size of statistics for the specified column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column.</param>
        /// <returns>The maximum size of statistics for the specified column.</returns>
        public ulong MaxStatisticsSize(ColumnPath path)
        {
            return ExceptionInfo.Return<ulong>(Handle, path.Handle, WriterProperties_Max_Statistics_Size);
        }

        /// <summary>
        /// Whether CRC checksums are written for data pages
        /// </summary>
        public bool PageChecksumEnabled => ExceptionInfo.Return<bool>(Handle, WriterProperties_Page_Checksum_Enabled);

        /// <summary>
        /// Gets the columns by which the data is sorted when writing to the file.
        /// </summary>
        /// <returns>
        /// An array of <see cref="SortingColumn"/> specifying the sorting order for the file
        /// </returns>
        public SortingColumn[] SortingColumns()
        {
            IntPtr columnIndicesPtr = IntPtr.Zero;
            IntPtr descendingPtr = IntPtr.Zero;
            IntPtr nullsFirstPtr = IntPtr.Zero;
            int numColumns = 0;

            try
            {
                ExceptionInfo.Check(WriterProperties_Sorting_Columns(
                    Handle.IntPtr,
                    ref columnIndicesPtr,
                    ref descendingPtr,
                    ref nullsFirstPtr,
                    ref numColumns));

                var columnIndices = new int[numColumns];
                var isDescending = new bool[numColumns];
                var nullsFirst = new bool[numColumns];

                // Read column indices
                Marshal.Copy(columnIndicesPtr, columnIndices, 0, numColumns);

                // Read descending flags 
                for (var i = 0; i < numColumns; ++i)
                {
                    isDescending[i] = Marshal.ReadByte(descendingPtr, i) != 0;
                }

                // Read nulls_first flags
                for (var i = 0; i < numColumns; ++i)
                {
                    nullsFirst[i] = Marshal.ReadByte(nullsFirstPtr, i) != 0;
                }

                // Create and return SortingColumn array
                var result = new SortingColumn[numColumns];
                for (var i = 0; i < numColumns; i++)
                {
                    result[i] = new SortingColumn(columnIndices[i], isDescending[i], nullsFirst[i]);
                }
                return result;
            }
            finally
            {
                WriterProperties_Sorting_Columns_Free(columnIndicesPtr, descendingPtr, nullsFirstPtr);
            }
        }

        internal readonly ParquetHandle Handle;

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Get_Default_Writer_Properties(out IntPtr writerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterProperties_Free(IntPtr writerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Created_By(IntPtr writerProperties, out IntPtr createdBy);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterProperties_Created_By_Free(IntPtr cstr);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Data_Pagesize(IntPtr writerProperties, out long dataPageSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Index_Encoding(IntPtr writerProperties, out Encoding encoding);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Page_Encoding(IntPtr writerProperties, out Encoding encoding);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Pagesize_Limit(IntPtr writerProperties, out long pagesizeLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Max_Row_Group_Length(IntPtr writerProperties, out long length);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Version(IntPtr writerProperties, out CppParquetVersion version);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Write_Batch_Size(IntPtr writerProperties, out long size);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Page_Index_Enabled(IntPtr writerProperties, out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Page_Index_Enabled_For_Path(IntPtr writerProperties, IntPtr path, out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Page_Checksum_Enabled(IntPtr writerProperties, out bool enabled);

        //[DllImport(ParquetDll.Name)]
        //private static extern IntPtr WriterProperties_Column_Properties(IntPtr writerProperties, IntPtr path, out IntPtr columnProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Compression(IntPtr writerProperties, IntPtr path, out Compression compression);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Compression_Level(IntPtr writerProperties, IntPtr path, out int compressionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Dictionary_Enabled(IntPtr writerProperties, IntPtr path, [MarshalAs(UnmanagedType.I1)] out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Encoding(IntPtr writerProperties, IntPtr path, out Encoding encoding);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_File_Encryption_Properties(IntPtr writerProperties, out IntPtr fileEncryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Statistics_Enabled(IntPtr writerProperties, IntPtr path, [MarshalAs(UnmanagedType.I1)] out bool enabled);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Max_Statistics_Size(IntPtr writerProperties, IntPtr path, [MarshalAs(UnmanagedType.I1)] out ulong maxStatisticsSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterProperties_Sorting_Columns(IntPtr writerProperties, ref IntPtr columnIndices, ref IntPtr descending, ref IntPtr nullsFirst, ref int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterProperties_Sorting_Columns_Free(IntPtr columnIndices, IntPtr descending, IntPtr nullsFirst);
    }
}
