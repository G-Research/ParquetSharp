using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Represents the metadata for a row group in a Parquet file.
    /// </summary>
    /// <remarks>
    /// A row group is a logical horizontal partition of a Parquet file that contains a set of column chunks.
    /// This class provides information about the columns and rows in the row group.
    /// </remarks>
    public sealed class RowGroupMetaData
    {
        internal RowGroupMetaData(IntPtr handle)
        {
            _handle = handle;
        }

        /// <summary>
        /// Get the number of columns in the row group.
        /// </summary>
        public int NumColumns => ExceptionInfo.Return<int>(_handle, RowGroupMetaData_Num_Columns);
        /// <summary>
        /// Get the number of rows in the row group.
        /// </summary>
        public long NumRows => ExceptionInfo.Return<long>(_handle, RowGroupMetaData_Num_Rows);
        /// <summary>
        /// Get the schema descriptor for the row group.
        /// </summary>
        /// <value>A <see cref="SchemaDescriptor"/> object that describes the schema of the row group.</value>
        public SchemaDescriptor Schema => _schema ??= new SchemaDescriptor(ExceptionInfo.Return<IntPtr>(_handle, RowGroupMetaData_Schema));
        /// <summary>
        /// Get the total size of the row group in bytes.
        /// </summary>
        public long TotalByteSize => ExceptionInfo.Return<long>(_handle, RowGroupMetaData_Total_Byte_Size);

        /// <summary>
        /// Gets the columns by which the data is sorted in this row group.
        /// </summary>
        /// <returns>
        /// An array of <see cref="WriterProperties.SortingColumn"/> specifying the sorting order for this row group,
        /// or an empty array if no sorting columns are defined.
        /// </returns>
        public WriterProperties.SortingColumn[] SortingColumns()
        {
            IntPtr columnIndicesPtr = IntPtr.Zero;
            IntPtr descendingPtr = IntPtr.Zero;
            IntPtr nullsFirstPtr = IntPtr.Zero;
            int numColumns = 0;

            try
            {
                ExceptionInfo.Check(RowGroupMetaData_Sorting_Columns(
                    _handle,
                    ref columnIndicesPtr,
                    ref descendingPtr,
                    ref nullsFirstPtr,
                    ref numColumns));

                var columnIndices = new int[numColumns];
                var isDescending = new bool[numColumns];
                var nullsFirst = new bool[numColumns];

                if (numColumns > 0)
                {
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
                }

                // Create and return SortingColumn array
                var result = new WriterProperties.SortingColumn[numColumns];
                for (var i = 0; i < numColumns; i++)
                {
                    result[i] = new WriterProperties.SortingColumn(columnIndices[i], isDescending[i], nullsFirst[i]);
                }
                return result;
            }
            finally
            {
                if (columnIndicesPtr != IntPtr.Zero || descendingPtr != IntPtr.Zero || nullsFirstPtr != IntPtr.Zero)
                {
                    RowGroupMetaData_Sorting_Columns_Free(columnIndicesPtr, descendingPtr, nullsFirstPtr);
                }
            }
        }

        /// <summary>
        /// Get the metadata for the column chunk at the specified index.
        /// </summary>
        /// <param name="i">The index of the column chunk.</param>
        /// <returns>The metadata for the column chunk.</returns>
        /// <remarks>
        /// Column chunks are stored in the same order as the columns in the schema.
        /// </remarks>
        public ColumnChunkMetaData GetColumnChunkMetaData(int i)
        {
            return new ColumnChunkMetaData(ExceptionInfo.Return<int, IntPtr>(_handle, i, RowGroupMetaData_Get_Column_Chunk_Meta_Data));
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Get_Column_Chunk_Meta_Data(IntPtr rowGroupMetaData, int i, out IntPtr columnChunkMetaData);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Num_Columns(IntPtr rowGroupMetaData, out int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Num_Rows(IntPtr rowGroupMetaData, out long numRows);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Schema(IntPtr rowGroupMetaData, out IntPtr schemaDescriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Total_Byte_Size(IntPtr rowGroupMetaData, out long totalByteSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupMetaData_Sorting_Columns(IntPtr rowGroupMetaData, ref IntPtr columnIndices, ref IntPtr descending, ref IntPtr nullsFirst, ref int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern void RowGroupMetaData_Sorting_Columns_Free(IntPtr columnIndices, IntPtr descending, IntPtr nullsFirst);

        private readonly IntPtr _handle;
        private SchemaDescriptor? _schema;
    }
}
