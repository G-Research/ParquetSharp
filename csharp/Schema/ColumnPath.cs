using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.Schema
{
    public sealed class ColumnPath : IDisposable
    {
        public ColumnPath(string[] dotVector)
            : this(Make(dotVector))
        {
        }

        public ColumnPath(string dotString)
            : this(Make(dotString))
        {
        }

        public ColumnPath(Node node)
            : this(Make(node))
        {
        }

        internal ColumnPath(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, ColumnPath_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public ColumnPath Extend(string nodeName)
        {
            return new ColumnPath(ExceptionInfo.Return<string, IntPtr>(Handle, nodeName, ColumnPath_Extend));
        }

        public string ToDotString()
        {
            ExceptionInfo.Check(ColumnPath_ToDotString(Handle, out var cstr));

            var dotString = Marshal.PtrToStringAnsi(cstr);
            ColumnPath_ToDotString_Free(cstr);

            return dotString;
        }

        public unsafe string[] ToDotVector()
        {
            ExceptionInfo.Check(ColumnPath_ToDotVector(Handle, out var dotVector, out var length));

            var strings = new string[length];
            var cstrings = (IntPtr*) dotVector.ToPointer();

            for (var i = 0; i != length; ++i)
            {
                strings[i] = Marshal.PtrToStringAnsi(cstrings[i]);
            }

            ColumnPath_ToDotVector_Free(dotVector, length);

            return strings;
        }
        
        internal readonly ParquetHandle Handle;

        private static IntPtr Make(string[] dotVector)
        {
            ExceptionInfo.Check(ColumnPath_Make(dotVector, dotVector.Length, out var handle));
            return handle;
        }

        private static IntPtr Make(string dotString)
        {
            ExceptionInfo.Check(ColumnPath_MakeFromDotString(dotString, out var handle));
            return handle;
        }

        private static IntPtr Make(Node node)
        {
            ExceptionInfo.Check(ColumnPath_MakeFromNode(node.Handle, out var handle));
            return handle;
        }

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnPath_Free(IntPtr columnPath);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnPath_Make(string[] path, int length, out IntPtr columnPath);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnPath_MakeFromDotString(string dotString, out IntPtr columnPath);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnPath_MakeFromNode(IntPtr node, out IntPtr columnPath);

        [DllImport(ParquetDll.Name, CharSet = CharSet.Ansi)]
        private static extern IntPtr ColumnPath_Extend(IntPtr columnPath, string nodeName, out IntPtr newColumnPath);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnPath_ToDotString(IntPtr columnPath, out IntPtr dotString);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnPath_ToDotString_Free(IntPtr dotString);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnPath_ToDotVector(IntPtr columnPath, out IntPtr dotVector, out int length);

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnPath_ToDotVector_Free(IntPtr dotVector, int length);
    }
}