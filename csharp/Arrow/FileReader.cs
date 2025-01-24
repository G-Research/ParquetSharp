using System;
using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using ParquetSharp.IO;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Reads Parquet files using the Arrow format.
    /// </summary>
    public class FileReader : IDisposable
    {
#pragma warning disable RS0026

        /// <summary>
        /// Create a new Arrow FileReader for a file at the specified path
        /// </summary>
        /// <param name="path">Path to the Parquet file</param>
        /// <param name="properties">Parquet reader properties</param>
        /// <param name="arrowProperties">Arrow specific reader properties</param>
        public FileReader(
            string path,
            ReaderProperties? properties = null,
            ArrowReaderProperties? arrowProperties = null)
        {
            using var defaultProperties = properties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var readerProperties = properties ?? defaultProperties!;

            var arrowPropertiesPtr =
                arrowProperties == null ? IntPtr.Zero : arrowProperties.Handle.IntPtr;

            ExceptionInfo.Check(FileReader_OpenPath(path, readerProperties.Handle.IntPtr, arrowPropertiesPtr, out var reader));

            _handle = new ParquetHandle(reader, FileReader_Free);

            GC.KeepAlive(properties);
            GC.KeepAlive(arrowProperties);
        }

        /// <summary>
        /// Create a new Arrow FileReader for a file object
        /// </summary>
        /// <param name="file">The file to read</param>
        /// <param name="properties">Parquet reader properties</param>
        /// <param name="arrowProperties">Arrow specific reader properties</param>
        /// <exception cref="ArgumentNullException">Thrown if the file or its handle are null</exception>
        public FileReader(
            RandomAccessFile file,
            ReaderProperties? properties = null,
            ArrowReaderProperties? arrowProperties = null)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (file.Handle == null) throw new ArgumentNullException(nameof(file.Handle));

            using var defaultProperties = properties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var readerProperties = properties ?? defaultProperties!;

            var arrowPropertiesPtr =
                arrowProperties == null ? IntPtr.Zero : arrowProperties.Handle.IntPtr;

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr, IntPtr, IntPtr>(
                file.Handle, readerProperties.Handle.IntPtr, arrowPropertiesPtr, FileReader_OpenFile), FileReader_Free);
            _randomAccessFile = file;

            GC.KeepAlive(properties);
            GC.KeepAlive(arrowProperties);
        }

        /// <summary>
        /// Create a new Arrow FileReader for a .NET stream
        /// </summary>
        /// <param name="stream">The stream to read</param>
        /// <param name="properties">Parquet reader properties</param>
        /// <param name="arrowProperties">Arrow specific reader properties</param>
        /// <param name="leaveOpen">Whether to keep the stream open after the reader is closed</param>
        /// <exception cref="ArgumentNullException">Thrown if the file or its handle are null</exception>
        public FileReader(
            Stream stream,
            ReaderProperties? properties = null,
            ArrowReaderProperties? arrowProperties = null,
            bool leaveOpen = false)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));

            using var defaultProperties = properties == null ? ReaderProperties.GetDefaultReaderProperties() : null;
            var readerProperties = properties ?? defaultProperties!;

            _randomAccessFile = new ManagedRandomAccessFile(stream, leaveOpen);
            _ownedFile = true;

            var arrowPropertiesPtr =
                arrowProperties == null ? IntPtr.Zero : arrowProperties.Handle.IntPtr;

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr, IntPtr, IntPtr>(
                _randomAccessFile.Handle!, readerProperties.Handle.IntPtr, arrowPropertiesPtr, FileReader_OpenFile), FileReader_Free);

            GC.KeepAlive(properties);
            GC.KeepAlive(arrowProperties);
        }

#pragma warning disable RS0026

        /// <summary>
        /// The Arrow schema of the file being read
        /// </summary>
        public unsafe Apache.Arrow.Schema Schema
        {
            get
            {
                var cSchema = new CArrowSchema();
                ExceptionInfo.Check(FileReader_GetSchema(_handle.IntPtr, &cSchema));
                return CArrowSchemaImporter.ImportSchema(&cSchema);
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
            var cStream = new CArrowArrayStream();
            fixed (int* rowGroupsPtr = rowGroups)
            {
                fixed (int* columnsPtr = columns)
                {
                    ExceptionInfo.Check(FileReader_GetRecordBatchReader(
                        _handle.IntPtr, rowGroupsPtr, rowGroups?.Length ?? 0, columnsPtr, columns?.Length ?? 0, &cStream));
                }
            }
            GC.KeepAlive(_handle);
            return CArrowArrayStreamImporter.ImportArrayStream(&cStream);
        }

        /// <summary>
        /// Get the underlying ParquetFileReader used by this Arrow FileReader
        /// </summary>
        public ParquetFileReader ParquetReader
        {
            get
            {
                var readerPtr = ExceptionInfo.Return<IntPtr>(_handle, FileReader_ParquetReader);
                return new ParquetFileReader(new ChildParquetHandle(readerPtr, _handle));
            }
        }

        /// <summary>
        /// Get the schema manifest, which describes the relationship between the Arrow schema and Parquet schema
        /// </summary>
        public SchemaManifest SchemaManifest
        {
            get
            {
                var manifestPtr = ExceptionInfo.Return<IntPtr>(_handle, FileReader_Manifest);
                return new SchemaManifest(new ChildParquetHandle(manifestPtr, _handle));
            }
        }

        public void Dispose()
        {
            _handle.Dispose();
            if (_ownedFile)
            {
                _randomAccessFile?.Dispose();
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_OpenPath(
            [MarshalAs(UnmanagedType.LPUTF8Str)] string path, IntPtr properties, IntPtr arrowProperties, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_OpenFile(
            IntPtr file, IntPtr properties, IntPtr arrowProperties, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileReader_GetSchema(IntPtr reader, CArrowSchema* schema);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_NumRowGroups(IntPtr reader, out int numRowGroups);

        [DllImport(ParquetDll.Name)]
        private static extern unsafe IntPtr FileReader_GetRecordBatchReader(
            IntPtr reader, int* rowGroups, int rowGroupsCount, int* columns, int columnsCount, CArrowArrayStream* stream);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_ParquetReader(IntPtr reader, out IntPtr parquetReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_Manifest(IntPtr reader, out IntPtr manifest);

        [DllImport(ParquetDll.Name)]
        private static extern void FileReader_Free(IntPtr reader);

        private readonly ParquetHandle _handle;
        private readonly RandomAccessFile? _randomAccessFile; // Keep a handle to the input file to prevent GC
        private readonly bool _ownedFile; // Whether this reader created the RandomAccessFile
    }
}
