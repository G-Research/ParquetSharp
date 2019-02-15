
using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Marshaling logic for exceptions from the native C/C++ code to the managed layer.
    /// </summary>
    internal sealed class ExceptionInfo
    {
        public delegate IntPtr GetAction<TValue>(out TValue value);
        public delegate IntPtr GetFunction<TValue>(IntPtr handle, out TValue value);
        public delegate IntPtr GetFunction<in TArg0, TValue>(IntPtr handle, TArg0 arg0, out TValue value);

        public static void Check(IntPtr exceptionInfo)
        {
            if (exceptionInfo == IntPtr.Zero)
            {
                return;
            }

            var type = Marshal.PtrToStringAnsi(ExceptionInfo_Type(exceptionInfo));
            var message = Marshal.PtrToStringAnsi(ExceptionInfo_Message(exceptionInfo));

            ExceptionInfo_Free(exceptionInfo);

            throw new ParquetException(type, message);
        }

        public static TValue Return<TValue>(GetAction<TValue> getter)
        {
            Check(getter(out var value));
            return value;
        }

        public static TValue Return<TValue>(ParquetHandle handle, GetFunction<TValue> getter)
        {
            var value = Return(handle.IntPtr, getter);
            GC.KeepAlive(handle);
            return value;
        }

        public static TValue Return<TValue>(IntPtr handle, GetFunction<TValue> getter)
        {
            Check(getter(handle, out var value));
            return value;
        }

        public static TValue Return<TValue>(ParquetHandle handle, ParquetHandle arg0, GetFunction<IntPtr, TValue> getter)
        {
            var value = Return(handle.IntPtr, arg0.IntPtr, getter);
            GC.KeepAlive(handle);
            return value;
        }

        public static TValue Return<TArg0, TValue>(ParquetHandle handle, TArg0 arg0, GetFunction<TArg0, TValue> getter)
        {
            var value = Return(handle.IntPtr, arg0, getter);
            GC.KeepAlive(handle);
            return value;
        }

        public static TValue Return<TArg0, TValue>(IntPtr handle, TArg0 arg0, GetFunction<TArg0, TValue> getter)
        {
            Check(getter(handle, arg0, out var value));
            return value;
        }

        public static string ReturnString(ParquetHandle handle, GetFunction<IntPtr> getter, Action<IntPtr> deleter = null)
        {
            Check(getter(handle.IntPtr, out var value));
            var str = Marshal.PtrToStringAnsi(value);
            deleter?.Invoke(value);
            GC.KeepAlive(handle);
            return str;
        }

        [DllImport(ParquetDll.Name)]
        private static extern void ExceptionInfo_Free(IntPtr exceptionInfo);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ExceptionInfo_Type(IntPtr exceptionInfo);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ExceptionInfo_Message(IntPtr exceptionInfo);
    }
}