using System;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using ParquetSharp.IO;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Reads Parquet files using the Arrow format
    /// </summary>
    public class FileReader : IDisposable
    {
        public FileReader(string path)
        {
            ExceptionInfo.Check(FileReader_OpenPath(path, out var reader));
            _handle = new ParquetHandle(reader, FileReader_Free);
        }

        public FileReader(RandomAccessFile file)
        {
            if (file == null) throw new ArgumentNullException(nameof(file));
            if (file.Handle == null) throw new ArgumentNullException(nameof(file.Handle));

            _handle = new ParquetHandle(ExceptionInfo.Return<IntPtr>(file.Handle, FileReader_OpenFile), FileReader_Free);
        }

        /// <summary>
        /// Get the Arrow schema of the file being read
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

        public void Dispose()
        {
            _handle.Dispose();
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_OpenPath([MarshalAs(UnmanagedType.LPUTF8Str)] string path, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_OpenFile(IntPtr file, out IntPtr reader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr FileReader_GetSchema(IntPtr reader, IntPtr schema);

        [DllImport(ParquetDll.Name)]
        private static extern void FileReader_Free(IntPtr reader);

        private readonly ParquetHandle _handle;
    }
}
