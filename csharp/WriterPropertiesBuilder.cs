using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ParquetSharp.Schema;

namespace ParquetSharp
{
    /// <summary>
    /// Builder pattern for creating and configuring a <see cref="WriterProperties"/> object. 
    /// </summary>
    public sealed class WriterPropertiesBuilder : IDisposable
    {
        /// <summary>
        /// Create a new WriterPropertiesBuilder.
        /// </summary>
        public WriterPropertiesBuilder()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Create(out var handle));
            _handle = new ParquetHandle(handle, WriterPropertiesBuilder_Free);
            ApplyDefaults();
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Build the <see cref="WriterProperties"/> from the current state of the builder.
        /// </summary>
        /// <returns>The configured <see cref="WriterProperties"/> object.</returns>
        public WriterProperties Build()
        {
            return new WriterProperties(ExceptionInfo.Return<IntPtr>(_handle, WriterPropertiesBuilder_Build));
        }

        /// <summary>
        /// Disable dictionary encoding for all columns.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableDictionary()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable dictionary encoding for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to disable dictionary encoding for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableDictionary(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable dictionary encoding for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to disable dictionary encoding for.</param>
        public WriterPropertiesBuilder DisableDictionary(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Dictionary_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Enable dictionary encoding by default.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableDictionary()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary(_handle.IntPtr));
            return this;
        }

        /// <summary>
        /// Enable dictionary encoding for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to enable dictionary encoding for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableDictionary(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Enable dictionary encoding for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to enable dictionary encoding for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableDictionary(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Dictionary_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Disable statistics by default.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableStatistics()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable statistics for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to disable statistics for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableStatistics(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable statistics for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to disable statistics for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableStatistics(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Statistics_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Enable statistics by default.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableStatistics()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Enable statistics for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to enable statistics for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableStatistics(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Enable statistics for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to enable statistics for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableStatistics(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Statistics_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Set the compression codec to use for all columns.
        /// </summary>
        /// <param name="codec">The <see cref="ParquetSharp.Compression"/> codec to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Compression(Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression(_handle.IntPtr, codec));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the compression codec to use for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to set the compression codec for.</param>
        /// <param name="codec">The <see cref="ParquetSharp.Compression"/> codec to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Compression(string path, Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_By_Path(_handle.IntPtr, path, codec));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the compression codec to use for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to set the compression codec for.</param>
        /// <param name="codec">The <see cref="ParquetSharp.Compression"/> codec to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Compression(ColumnPath path, Compression codec)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr, codec));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Set the compression level to use for all columns.
        /// </summary>
        /// <param name="compressionLevel">The compression level to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder CompressionLevel(int compressionLevel)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_Level(_handle.IntPtr, compressionLevel));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the compression level to use for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to set the compression level for.</param>
        /// <param name="compressionLevel">The compression level to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder CompressionLevel(string path, int compressionLevel)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_Level_By_Path(_handle.IntPtr, path, compressionLevel));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the compression level to use for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to set the compression level for.</param>
        /// <param name="compressionLevel">The compression level to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder CompressionLevel(ColumnPath path, int compressionLevel)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Compression_Level_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr, compressionLevel));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Set an identifier for the entity that created the file.
        /// </summary>
        /// <param name="createdBy">The name of the entity that created the file.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder CreatedBy(string createdBy)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Created_By(_handle.IntPtr, createdBy));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the maximum size of a data page in bytes.
        /// </summary>
        /// <param name="pageSize">The maximum data page size to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DataPagesize(long pageSize)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Data_Pagesize(_handle.IntPtr, pageSize));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the maximum size of a dictionary page in bytes.
        /// </summary>
        /// <param name="dictionaryPagesizeLimit">The maximum dictionary page size to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DictionaryPagesizeLimit(long dictionaryPagesizeLimit)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Dictionary_Pagesize_Limit(_handle.IntPtr, dictionaryPagesizeLimit));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the encoding type to use for all columns.
        /// </summary>
        /// <param name="encoding">The <see cref="ParquetSharp.Encoding"/> to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Encoding(Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding(_handle.IntPtr, encoding));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the encoding type to use for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to set the encoding for.</param>
        /// <param name="encoding">The <see cref="ParquetSharp.Encoding"/> to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Encoding(string path, Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding_By_Path(_handle.IntPtr, path, encoding));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the encoding type to use for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to set the encoding for.</param>
        /// <param name="encoding">The <see cref="ParquetSharp.Encoding"/> to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Encoding(ColumnPath path, Encoding encoding)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encoding_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr, encoding));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Set the encryption properties to use for the file.
        /// </summary>
        /// <param name="fileEncryptionProperties">The <see cref="FileEncryptionProperties"/> to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Encryption(FileEncryptionProperties? fileEncryptionProperties)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Encryption(_handle.IntPtr, fileEncryptionProperties?.Handle.IntPtr ?? IntPtr.Zero));
            GC.KeepAlive(_handle);
            GC.KeepAlive(fileEncryptionProperties);
            return this;
        }

        /// <summary>
        /// Set the maximum size of a row group in bytes.
        /// </summary>
        /// <param name="maxRowGroupLength">The maximum row group size to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder MaxRowGroupLength(long maxRowGroupLength)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Max_Row_Group_Length(_handle.IntPtr, maxRowGroupLength));
            return this;
        }

        /// <summary>
        /// Set the Parquet version to use for the file.
        /// </summary>
        /// <param name="version">The <see cref="ParquetVersion"/> to use.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder Version(ParquetVersion version)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Version(_handle.IntPtr, version.ToCppEnum()));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the number of rows to write in a single batch.
        /// </summary>
        /// <param name="writeBatchSize">The number of rows to write in a single batch.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder WriteBatchSize(long writeBatchSize)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Write_Batch_Size(_handle.IntPtr, writeBatchSize));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Enable writing the page index by default
        ///
        /// The page index contains statistics for data pages and can be used to skip pages
        /// when scanning data in ordered and unordered columns.
        ///
        /// For more details, see https://github.com/apache/parquet-format/blob/master/PageIndex.md
        /// </summary>
        public WriterPropertiesBuilder EnableWritePageIndex()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Write_Page_Index(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Enable writing the page index for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to enable the page index for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableWritePageIndex(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Write_Page_Index_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Enable writing the page index for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to enable the page index for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnableWritePageIndex(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Write_Page_Index_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable writing the page index by default.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableWritePageIndex()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Write_Page_Index(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable writing the page index for a specific column.
        /// </summary>
        /// <param name="path">The <see cref="ColumnPath"/> of the column to disable the page index for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableWritePageIndex(ColumnPath path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Write_Page_Index_By_ColumnPath(_handle.IntPtr, path.Handle.IntPtr));
            GC.KeepAlive(_handle);
            GC.KeepAlive(path);
            return this;
        }

        /// <summary>
        /// Disable writing the page index for a specific column.
        /// </summary>
        /// <param name="path">The path of the column to disable the page index for.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisableWritePageIndex(string path)
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Write_Page_Index_By_Path(_handle.IntPtr, path));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Enable writing CRC checksums for data pages.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder EnablePageChecksum()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Enable_Page_Checksum(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Disable writing CRC checksums for data pages.
        /// </summary>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder DisablePageChecksum()
        {
            ExceptionInfo.Check(WriterPropertiesBuilder_Disable_Page_Checksum(_handle.IntPtr));
            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Set the sorting columns to describe how written data is ordered.
        ///
        /// Note that no sorting or validation is done automatically. It is your responsibility to ensure
        /// that data is written in the specified order. 
        /// </summary>
        /// <param name="sortingColumns">Array of SortingColumn specifications defining the sort order.</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder SortingColumns(WriterProperties.SortingColumn[] sortingColumns)
        {
            if (sortingColumns == null)
            {
                throw new ArgumentNullException(nameof(sortingColumns));
            }

            if (sortingColumns.Length == 0)
            {
                // If empty array, call with null pointers to clear sorting
                ExceptionInfo.Check(WriterPropertiesBuilder_Sorting_Columns(
                    _handle.IntPtr,
                    IntPtr.Zero,
                    IntPtr.Zero,
                    IntPtr.Zero,
                    0));
                GC.KeepAlive(_handle);
                return this;
            }

            // Extract arrays from SortingColumn array
            var columnIndices = new int[sortingColumns.Length];
            var isDescending = new bool[sortingColumns.Length];
            var nullsFirst = new bool[sortingColumns.Length];

            for (int i = 0; i < sortingColumns.Length; i++)
            {
                columnIndices[i] = sortingColumns[i].ColumnIndex;
                isDescending[i] = sortingColumns[i].IsDescending;
                nullsFirst[i] = sortingColumns[i].NullsFirst;
            }

            // Pin the arrays in memory using fixed statements
            unsafe
            {
                fixed (int* columnIndicesPtr = columnIndices)
                fixed (bool* isDescendingPtr = isDescending)
                fixed (bool* nullsFirstPtr = nullsFirst)
                {
                    ExceptionInfo.Check(WriterPropertiesBuilder_Sorting_Columns(
                        _handle.IntPtr,
                        (IntPtr) columnIndicesPtr,
                        (IntPtr) isDescendingPtr,
                        (IntPtr) nullsFirstPtr,
                        sortingColumns.Length));
                }
            }

            GC.KeepAlive(_handle);
            return this;
        }

        /// <summary>
        /// Specify the native memory pool to use for allocations in the writer.
        /// </summary>
        /// <param name="memoryPool">The memory pool to use</param>
        /// <returns>This builder instance.</returns>
        public WriterPropertiesBuilder MemoryPool(MemoryPool memoryPool)
        {
            WriterPropertiesBuilder_Memory_Pool(_handle.IntPtr, memoryPool.Handle);

            GC.KeepAlive(_handle);
            return this;
        }

        private void ApplyDefaults()
        {
            OnDefaultProperty(DefaultWriterProperties.EnableDictionary, enabled =>
            {
                if (enabled)
                {
                    EnableDictionary();
                }
                else
                {
                    DisableDictionary();
                }
            });

            OnDefaultProperty(DefaultWriterProperties.EnableStatistics, enabled =>
            {
                if (enabled)
                {
                    EnableStatistics();
                }
                else
                {
                    DisableStatistics();
                }
            });

            OnDefaultProperty(DefaultWriterProperties.Compression, compression => { Compression(compression); });

            OnDefaultProperty(DefaultWriterProperties.CompressionLevel, compressionLevel => { CompressionLevel(compressionLevel); });

            OnDefaultRefProperty(DefaultWriterProperties.CreatedBy, createdBy => { CreatedBy(createdBy); });

            OnDefaultProperty(DefaultWriterProperties.DataPagesize, dataPagesize => { DataPagesize(dataPagesize); });

            OnDefaultProperty(DefaultWriterProperties.DictionaryPagesizeLimit, dictionaryPagesizeLimit => { DictionaryPagesizeLimit(dictionaryPagesizeLimit); });

            OnDefaultProperty(DefaultWriterProperties.Encoding, encoding => { Encoding(encoding); });

            OnDefaultProperty(DefaultWriterProperties.MaxRowGroupLength, maxRowGroupLength => { MaxRowGroupLength(maxRowGroupLength); });

            OnDefaultProperty(DefaultWriterProperties.Version, version => { Version(version); });

            OnDefaultProperty(DefaultWriterProperties.WriteBatchSize, writeBatchSize => { WriteBatchSize(writeBatchSize); });

            OnDefaultProperty(DefaultWriterProperties.WritePageIndex, writePageIndex =>
            {
                if (writePageIndex)
                {
                    EnableWritePageIndex();
                }
                else
                {
                    DisableWritePageIndex();
                }
            });

            OnDefaultProperty(DefaultWriterProperties.PageChecksumEnabled, checksumEnabled =>
            {
                if (checksumEnabled)
                {
                    EnablePageChecksum();
                }
                else
                {
                    DisablePageChecksum();
                }
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void OnDefaultProperty<T>(T? defaultPropertyValue, Action<T> setProperty)
            where T : struct
        {
            if (defaultPropertyValue.HasValue)
            {
                setProperty(defaultPropertyValue.Value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void OnDefaultRefProperty<T>(T? defaultPropertyValue, Action<T> setProperty)
        {
            if (defaultPropertyValue != null)
            {
                setProperty(defaultPropertyValue);
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Create(out IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern void WriterPropertiesBuilder_Free(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Build(IntPtr builder, out IntPtr writerProperties);

        // Dictionary enable/disable

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Dictionary(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Dictionary_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Dictionary_By_ColumnPath(IntPtr builder, IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Dictionary(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Dictionary_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Dictionary_By_ColumnPath(IntPtr builder, IntPtr path);

        // Statistics enable/disable

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Statistics(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Statistics_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Statistics_By_ColumnPath(IntPtr builder, IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Statistics(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Statistics_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Statistics_By_ColumnPath(IntPtr builder, IntPtr path);

        // Other properties

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression(IntPtr builder, Compression codec);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, Compression codec);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_By_ColumnPath(IntPtr builder, IntPtr path, Compression codec);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_Level(IntPtr builder, int compressionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_Level_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, int compressionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Compression_Level_By_ColumnPath(IntPtr builder, IntPtr path, int compressionLevel);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Created_By(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string createdBy);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Data_Pagesize(IntPtr builder, long pgSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Dictionary_Pagesize_Limit(IntPtr builder, long dictionaryPsizeLimit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encoding(IntPtr builder, Encoding encodingType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encoding_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, Encoding encodingType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encoding_By_ColumnPath(IntPtr builder, IntPtr path, Encoding encodingType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Encryption(IntPtr builder, IntPtr fileEncryptionProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Max_Row_Group_Length(IntPtr builder, long maxRowGroupLength);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Version(IntPtr builder, CppParquetVersion version);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Write_Batch_Size(IntPtr builder, long writeBatchSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Write_Page_Index(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Write_Page_Index_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Write_Page_Index_By_ColumnPath(IntPtr builder, IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Write_Page_Index(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Write_Page_Index_By_Path(IntPtr builder, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Write_Page_Index_By_ColumnPath(IntPtr builder, IntPtr path);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Enable_Page_Checksum(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Disable_Page_Checksum(IntPtr builder);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Sorting_Columns(IntPtr builder, IntPtr columnIndices, IntPtr isDescending, IntPtr nullsFirst, int numColumns);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr WriterPropertiesBuilder_Memory_Pool(IntPtr builder, IntPtr memoryPool);

        private readonly ParquetHandle _handle;
    }
}
