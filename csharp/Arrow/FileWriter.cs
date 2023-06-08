using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using ParquetSharp.IO;
using Array = System.Array;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Writes Parquet files using Arrow format data
    /// </summary>
    public class FileWriter : IDisposable
    {
        /// <summary>
        /// Create a new Arrow FileWriter that writes to the specified path
        /// </summary>
        /// <param name="path">Path to the Parquet file to write</param>
        /// <param name="schema">Arrow schema for the data to be written</param>
        /// <param name="writerProperties">Parquet writer properties</param>
        /// <param name="arrowWriterProperties">Arrow specific writer properties</param>
        public unsafe FileWriter(
            string path,
            Apache.Arrow.Schema schema,
            WriterProperties? writerProperties = null,
            ArrowWriterProperties? arrowWriterProperties = null)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var writerPropertiesPtr =
                writerProperties == null ? IntPtr.Zero : writerProperties.Handle.IntPtr;

            var arrowWriterPropertiesPtr =
                arrowWriterProperties == null ? IntPtr.Zero : arrowWriterProperties.Handle.IntPtr;

            var cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportSchema(schema, cSchema);
                ExceptionInfo.Check(FileWriter_OpenPath(path, cSchema, writerPropertiesPtr, arrowWriterPropertiesPtr, out var writer));
                _handle = new ParquetHandle(writer, FileWriter_Free);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }

            GC.KeepAlive(writerProperties);
            GC.KeepAlive(arrowWriterProperties);
        }

        /// <summary>
        /// Create a new Arrow FileWriter that writes to the specified output stream
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="schema">Arrow schema for the data to be written</param>
        /// <param name="writerProperties">Parquet writer properties</param>
        /// <param name="arrowWriterProperties">Arrow specific writer properties</param>
        public unsafe FileWriter(
            OutputStream outputStream,
            Apache.Arrow.Schema schema,
            WriterProperties? writerProperties = null,
            ArrowWriterProperties? arrowWriterProperties = null)
        {
            if (outputStream == null) throw new ArgumentNullException(nameof(outputStream));
            if (outputStream.Handle == null) throw new ArgumentNullException(nameof(outputStream.Handle));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var writerPropertiesPtr =
                writerProperties == null ? IntPtr.Zero : writerProperties.Handle.IntPtr;

            var arrowWriterPropertiesPtr =
                arrowWriterProperties == null ? IntPtr.Zero : arrowWriterProperties.Handle.IntPtr;

            var cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportSchema(schema, cSchema);
                ExceptionInfo.Check(FileWriter_OpenStream(
                    outputStream.Handle.IntPtr, cSchema, writerPropertiesPtr, arrowWriterPropertiesPtr, out var writer));
                _handle = new ParquetHandle(writer, FileWriter_Free);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }

            GC.KeepAlive(outputStream);
            GC.KeepAlive(writerProperties);
            GC.KeepAlive(arrowWriterProperties);
        }

        /// <summary>
        /// The Arrow schema of the file being written
        /// </summary>
        public unsafe Apache.Arrow.Schema Schema
        {
            get
            {
                var cSchema = CArrowSchema.Create();
                try
                {
                    ExceptionInfo.Check(FileWriter_GetSchema(_handle.IntPtr, (IntPtr) cSchema));
                    return CArrowSchemaImporter.ImportSchema(cSchema);
                }
                finally
                {
                    CArrowSchema.Free(cSchema);
                }
            }
        }

        /// <summary>
        /// Write an Arrow table to Parquet
        ///
        /// The table data will be chunked into row groups that respect the maximum
        /// chunk size specified if required.
        /// This method requires tha the columns in the table use equal chunking.
        /// </summary>
        /// <param name="table">The table to write</param>
        /// <param name="chunkSize">The maximum length of row groups to write</param>
        public void WriteTable(Table table, long chunkSize = 1024 * 1024)
        {
            var arrayStream = new RecordBatchStream(table);
            WriteRecordBatchStream(arrayStream, chunkSize);
        }

        /// <summary>
        /// Write a record batch to Parquet
        ///
        /// The data will be chunked into row groups that respect the maximum
        /// chunk size specified if required.
        /// </summary>
        /// <param name="recordBatch">The record batch to write</param>
        /// <param name="chunkSize">The maximum length of row groups to write</param>
        public void WriteRecordBatch(RecordBatch recordBatch, long chunkSize = 1024 * 1024)
        {
            var arrayStream = new RecordBatchStream(recordBatch.Schema, new[] {recordBatch});
            WriteRecordBatchStream(arrayStream, chunkSize);
        }

        /// <summary>
        /// Close the file writer, writing the Parquet footer.
        /// This is the recommended way of closing Parquet files, rather than relying on the Dispose() method,
        /// as the latter will gobble exceptions.
        /// </summary>
        public void Close()
        {
            ExceptionInfo.Check(FileWriter_Close(_handle.IntPtr));
            GC.KeepAlive(_handle);
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        /// <summary>
        /// Write record batch stream to Parquet
        ///
        /// This will build a table internally so requires all data in memory at once.
        /// </summary>
        private unsafe void WriteRecordBatchStream(IArrowArrayStream arrayStream, long chunkSize)
        {
            var cArrayStream = CArrowArrayStream.Create();
            try
            {
                CArrowArrayStreamExporter.ExportArrayStream(arrayStream, cArrayStream);
                ExceptionInfo.Check(FileWriter_WriteTable(_handle.IntPtr, cArrayStream, chunkSize));
            }
            finally
            {
                CArrowArrayStream.Free(cArrayStream);
            }
            GC.KeepAlive(_handle);
        }

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_OpenPath(
            [MarshalAs(UnmanagedType.LPUTF8Str)] string path, CArrowSchema* schema, IntPtr properties, IntPtr arrowProperties, out IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_OpenStream(
            IntPtr outputStream, CArrowSchema* schema, IntPtr properties, IntPtr arrowProperties, out IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileWriter_GetSchema(IntPtr writer, IntPtr schema);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_WriteTable(IntPtr writer, CArrowArrayStream* stream, long chunkSize);

        [DllImport(ParquetDll.Name)]
        private static extern void FileWriter_Free(IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileWriter_Close(IntPtr writer);

        private readonly ParquetHandle _handle;

        private sealed class RecordBatchStream : IArrowArrayStream
        {
            public RecordBatchStream(Apache.Arrow.Schema schema, RecordBatch[] batches)
            {
                Schema = schema;
                _batches = batches;
            }

            public RecordBatchStream(Table table)
            {
                Schema = table.Schema;
                _batches = TableToRecordBatches(table);
            }

            public Apache.Arrow.Schema Schema { get; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_index < 0)
                {
                    throw new ObjectDisposedException(nameof(RecordBatchStream));
                }

                RecordBatch? result = _index < _batches.Length ? _batches[_index++] : null;
                return new ValueTask<RecordBatch?>(result);
            }

            public void Dispose()
            {
                _index = -1;
            }

            /// <summary>
            /// Convert a table to an array of record batches, assuming all columns are chunked equally
            /// </summary>
            private static RecordBatch[] TableToRecordBatches(Table table)
            {
                if (table.RowCount == 0)
                {
                    return Array.Empty<RecordBatch>();
                }

                if (table.ColumnCount == 0)
                {
                    throw new ArgumentException("No columns in table");
                }

                var column = table.Column(0);
                var chunkCount = column.Data.ArrayCount;
                var chunkSizes = Enumerable.Range(0, chunkCount).Select(i => column.Data.Array(i).Length).ToArray();

                for (var columnIdx = 1; columnIdx < table.ColumnCount; ++columnIdx)
                {
                    column = table.Column(columnIdx);
                    var columnChunkCount = column.Data.ArrayCount;
                    if (columnChunkCount != chunkCount)
                    {
                        throw new Exception(
                            "Cannot convert table to record batches, arrays do not have the same number of chunks");
                    }
                    var columnChunkSizes = Enumerable.Range(0, chunkCount).Select(i => column.Data.Array(i).Length).ToArray();
                    if (!columnChunkSizes.SequenceEqual(chunkSizes))
                    {
                        throw new Exception(
                            "Cannot convert table to record batches, arrays do not have the chunk sizes");
                    }
                }

                var batches = new RecordBatch[chunkCount];
                for (var batchIdx = 0; batchIdx < chunkCount; ++batchIdx)
                {
                    var arrays = Enumerable.Range(0, table.ColumnCount).Select(i => table.Column(i).Data.Array(batchIdx)).ToArray();
                    batches[batchIdx] = new RecordBatch(table.Schema, arrays, chunkSizes[batchIdx]);
                }

                return batches;
            }

            private readonly RecordBatch[] _batches;
            private int _index = 0;
        }
    }
}
