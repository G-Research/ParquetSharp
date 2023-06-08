using System;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using ParquetSharp.IO;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Reads Parquet files using the Arrow format
    /// </summary>
    public class FileReader : IDisposable
    {
        /// <summary>
        /// Create a new Arrow FileReader for a file at the specified path
        /// </summary>
        /// <param name="path">Path to the Parquet file</param>
        /// <param name="readerProperties">Parquet reader properties</param>
        /// <param name="arrowReaderProperties">Arrow specific reader properties</param>
        public FileReader(
            string path,
            ReaderProperties? readerProperties = null,
            ArrowReaderProperties? arrowReaderProperties = null)
        {
            using var defaultProperties = readerProperties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var properties = readerProperties ?? defaultProperties!;

            var arrowReaderPropertiesPtr =
                arrowReaderProperties == null ? IntPtr.Zero : arrowReaderProperties.Handle.IntPtr;

            ExceptionInfo.Check(FileReader_OpenPath(path, properties.Handle.IntPtr, arrowReaderPropertiesPtr, out var reader));

            _handle = new ParquetHandle(reader, FileReader_Free);

            GC.KeepAlive(readerProperties);
            GC.KeepAlive(arrowReaderProperties);
        }

        /// <summary>
        /// Create a new Arrow FileReader for a file object
        /// </summary>
        /// <param name="file">The file to read</param>
        /// <param name="readerProperties">Parquet reader properties</param>
        /// <param name="arrowReaderProperties">Arrow specific reader properties</param>
        /// <exception cref="ArgumentNullException">Thrown if the file or its handle are null</exception>
        public FileReader(
            RandomAccessFile file,
            ReaderProperties? readerProperties = null,
            ArrowReaderProperties? arrowReaderProperties = null)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (file.Handle == null) throw new ArgumentNullException(nameof(file.Handle));

            using var defaultProperties = readerProperties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var properties = readerProperties ?? defaultProperties!;

            var arrowReaderPropertiesPtr =
                arrowReaderProperties == null ? IntPtr.Zero : arrowReaderProperties.Handle.IntPtr;

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr, IntPtr, IntPtr>(
                file.Handle, properties.Handle.IntPtr, arrowReaderPropertiesPtr, FileReader_OpenFile), FileReader_Free);

            GC.KeepAlive(readerProperties);
            GC.KeepAlive(arrowReaderProperties);
        }

        /// <summary>
        /// The Arrow schema of the file being read
        /// </summary>
        public unsafe Apache.Arrow.Schema Schema
        {
            get
            {
                var cSchema = CArrowSchema.Create();
                try
                {
                    ExceptionInfo.Check(FileReader_GetSchema(_handle.IntPtr, (IntPtr) cSchema));
                    return CArrowSchemaImporter.ImportSchema(cSchema);
                }
                finally
                {
                    CArrowSchema.Free(cSchema);
                }
            }
        }

        /// <summary>
        /// The number of row groups in the file
        /// </summary>
        public int NumRowGroups => ExceptionInfo.Return<int>(_handle, FileReader_NumRowGroups);

        /// <summary>
        /// Get a record batch reader for the file data
        /// <param name="rowGroups">The indices of row groups to read data from</param>
        /// <param name="columns">The indices of columns to read, based on the schema</param>
        /// </summary>
        public unsafe IArrowArrayStream GetRecordBatchReader(
            int[]? rowGroups = null,
            int[]? columns = null)
        {
            var cStream = CArrowArrayStream.Create();
            try
            {
                fixed (int* rowGroupsPtr = rowGroups)
                {
                    fixed (int* columnsPtr = columns)
                    {
                        ExceptionInfo.Check(FileReader_GetRecordBatchReader(
                            _handle.IntPtr, rowGroupsPtr, rowGroups?.Length ?? 0, columnsPtr, columns?.Length ?? 0, (IntPtr) cStream));
                    }
                }
                GC.KeepAlive(_handle);
                return CArrowArrayStreamImporter.ImportArrayStream(cStream);
            }
            catch
            {
                // FIXME: Can create cStream on the stack after https://github.com/apache/arrow/pull/35996
                CArrowArrayStream.Free(cStream);
                throw;
            }
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_OpenPath(
            [MarshalAs(UnmanagedType.LPUTF8Str)] string path, IntPtr properties, IntPtr arrowProperties, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_OpenFile(
            IntPtr file, IntPtr properties, IntPtr arrowProperties, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_GetSchema(IntPtr reader, IntPtr schema);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_NumRowGroups(IntPtr reader, out int numRowGroups);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileReader_GetRecordBatchReader(
            IntPtr reader, int* rowGroups, int rowGroupsCount, int* columns, int columnsCount, IntPtr stream);

        [DllImport(ParquetDll.Name)]
        private static extern void FileReader_Free(IntPtr reader);

        private readonly ParquetHandle _handle;
    }
}
