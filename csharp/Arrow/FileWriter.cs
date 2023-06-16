using System;
using System.IO;
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
    ///
    /// This may be used to write whole tables or record batches,
    /// using the WriteTable or WriteRecordBatch methods.
    ///
    /// For more control over writing, you can create a new row group with NewRowGroup,
    /// then write all columns for the row group with the WriteColumn method.
    /// All required columns must be written before starting the next row group
    /// or closing the file.
    /// </summary>
    public class FileWriter : IDisposable
    {
        /// <summary>
        /// Create a new Arrow FileWriter that writes to the specified path
        /// </summary>
        /// <param name="path">Path to the Parquet file to write</param>
        /// <param name="schema">Arrow schema for the data to be written</param>
        /// <param name="properties">Parquet writer properties</param>
        /// <param name="arrowProperties">Arrow specific writer properties</param>
        public unsafe FileWriter(
            string path,
            Apache.Arrow.Schema schema,
            WriterProperties? properties = null,
            ArrowWriterProperties? arrowProperties = null)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var writerPropertiesPtr =
                properties == null ? IntPtr.Zero : properties.Handle.IntPtr;

            var arrowPropertiesPtr =
                arrowProperties == null ? IntPtr.Zero : arrowProperties.Handle.IntPtr;

            var cSchema = new CArrowSchema();
            CArrowSchemaExporter.ExportSchema(schema, &cSchema);
            ExceptionInfo.Check(FileWriter_OpenPath(path, &cSchema, writerPropertiesPtr, arrowPropertiesPtr, out var writer));
            _handle = new ParquetHandle(writer, FileWriter_Free);

            GC.KeepAlive(properties);
            GC.KeepAlive(arrowProperties);
        }

        /// <summary>
        /// Create a new Arrow FileWriter that writes to the specified output stream
        /// </summary>
        /// <param name="outputStream">Stream to write to</param>
        /// <param name="schema">Arrow schema for the data to be written</param>
        /// <param name="properties">Parquet writer properties</param>
        /// <param name="arrowProperties">Arrow specific writer properties</param>
        public unsafe FileWriter(
            OutputStream outputStream,
            Apache.Arrow.Schema schema,
            WriterProperties? properties = null,
            ArrowWriterProperties? arrowProperties = null)
        {
            if (outputStream == null) throw new ArgumentNullException(nameof(outputStream));
            if (outputStream.Handle == null) throw new ArgumentNullException(nameof(outputStream.Handle));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var writerPropertiesPtr =
                properties == null ? IntPtr.Zero : properties.Handle.IntPtr;

            var arrowPropertiesPtr =
                arrowProperties == null ? IntPtr.Zero : arrowProperties.Handle.IntPtr;

            var cSchema = new CArrowSchema();
            CArrowSchemaExporter.ExportSchema(schema, &cSchema);
            ExceptionInfo.Check(FileWriter_OpenStream(
                outputStream.Handle.IntPtr, &cSchema, writerPropertiesPtr, arrowPropertiesPtr, out var writer));
            _handle = new ParquetHandle(writer, FileWriter_Free);
            _outputStream = outputStream;

            GC.KeepAlive(properties);
            GC.KeepAlive(arrowProperties);
        }

        /// <summary>
        /// Create a new Arrow FileWriter that writes to a .NET stream
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        /// <param name="schema">Arrow schema for the data to be written</param>
        /// <param name="properties">Parquet writer properties</param>
        /// <param name="arrowProperties">Arrow specific writer properties</param>
        /// <param name="leaveOpen">Whether to keep the stream open after closing the writer</param>
        public unsafe FileWriter(
            Stream stream,
            Apache.Arrow.Schema schema,
            WriterProperties? properties = null,
            ArrowWriterProperties? arrowProperties = null,
            bool leaveOpen = false)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var writerPropertiesPtr =
                properties == null ? IntPtr.Zero : properties.Handle.IntPtr;

            var arrowPropertiesPtr =
                arrowProperties == null ? IntPtr.Zero : arrowProperties.Handle.IntPtr;

            _outputStream = new ManagedOutputStream(stream, leaveOpen);
            _ownedStream = true;

            var cSchema = new CArrowSchema();
            CArrowSchemaExporter.ExportSchema(schema, &cSchema);
            ExceptionInfo.Check(FileWriter_OpenStream(
                _outputStream.Handle!.IntPtr, &cSchema, writerPropertiesPtr, arrowPropertiesPtr, out var writer));
            _handle = new ParquetHandle(writer, FileWriter_Free);

            GC.KeepAlive(properties);
            GC.KeepAlive(arrowProperties);
        }

        /// <summary>
        /// The Arrow schema of the file being written
        /// </summary>
        public unsafe Apache.Arrow.Schema Schema
        {
            get
            {
                var cSchema = new CArrowSchema();
                ExceptionInfo.Check(FileWriter_GetSchema(_handle.IntPtr, &cSchema));
                return CArrowSchemaImporter.ImportSchema(&cSchema);
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
        /// Start writing a new row group to the file. After calling this method,
        /// each column required in the schema must be written using WriteColumn
        /// before creating a new row group or closing the file.
        /// </summary>
        /// <param name="chunkSize">The number of rows to be written in this row group</param>
        public void NewRowGroup(long chunkSize)
        {
            ExceptionInfo.Check(FileWriter_NewRowGroup(_handle.IntPtr, chunkSize));
            GC.KeepAlive(_handle);
        }

        /// <summary>
        /// Write a column of data to a row group using an Arrow Array
        /// </summary>
        /// <param name="array">The array of data for the column</param>
        public unsafe void WriteColumnChunk(IArrowArray array)
        {
            var cArray = new CArrowArray();
            var cType = new CArrowSchema();

            CArrowArrayExporter.ExportArray(array, &cArray);
            CArrowSchemaExporter.ExportType(array.Data.DataType, &cType);
            ExceptionInfo.Check(FileWriter_WriteColumnChunk(_handle.IntPtr, &cArray, &cType));

            GC.KeepAlive(_handle);
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
            if (_ownedStream)
            {
                _outputStream?.Dispose();
            }
        }

        /// <summary>
        /// Write record batch stream to Parquet
        ///
        /// This will build a table internally so requires all data in memory at once.
        /// </summary>
        private unsafe void WriteRecordBatchStream(IArrowArrayStream arrayStream, long chunkSize)
        {
            var cArrayStream = new CArrowArrayStream();
            CArrowArrayStreamExporter.ExportArrayStream(arrayStream, &cArrayStream);
            ExceptionInfo.Check(FileWriter_WriteTable(_handle.IntPtr, &cArrayStream, chunkSize));
            GC.KeepAlive(_handle);
        }

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_OpenPath(
            [MarshalAs(UnmanagedType.LPUTF8Str)] string path, CArrowSchema* schema, IntPtr properties, IntPtr arrowProperties, out IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_OpenStream(
            IntPtr outputStream, CArrowSchema* schema, IntPtr properties, IntPtr arrowProperties, out IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_GetSchema(IntPtr writer, CArrowSchema* schema);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_WriteTable(IntPtr writer, CArrowArrayStream* stream, long chunkSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileWriter_NewRowGroup(IntPtr writer, long chunkSize);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileWriter_WriteColumnChunk(IntPtr writer, CArrowArray* array, CArrowSchema* arrayType);

        [DllImport(ParquetDll.Name)]
        private static extern void FileWriter_Free(IntPtr writer);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileWriter_Close(IntPtr writer);

        private readonly ParquetHandle _handle;
        private readonly OutputStream? _outputStream; // Keep a handle to the output stream to prevent GC
        private readonly bool _ownedStream; // Whether this writer created the OutputStream

        /// <summary>
        /// A stream of record batches where batches are all stored in memory
        /// </summary>
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
                    // This shouldn't happen, as the row count should be zero when there are no columns
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
