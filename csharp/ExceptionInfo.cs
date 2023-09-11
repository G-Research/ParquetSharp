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
        public delegate IntPtr GetAction<in TArg0, TValue>(TArg0 arg0, out TValue value);
        public delegate IntPtr GetAction<in TArg0, in TArg1, TValue>(TArg0 arg0, TArg1 arg1, out TValue value);
        public delegate IntPtr GetAction<in TArg0, in TArg1, in TArg2, TValue>(TArg0 arg0, TArg1 arg1, TArg2 arg2, out TValue value);
        public delegate IntPtr GetFunction<TValue>(IntPtr handle, out TValue value);
        public delegate IntPtr GetFunction<in TArg0, TValue>(IntPtr handle, TArg0 arg0, out TValue value);
        public delegate IntPtr GetFunction<in TArg0, in TArg1, TValue>(IntPtr handle, TArg0 arg0, TArg1 arg1, out TValue value);

        public static void Check(IntPtr exceptionInfo)
        {
            if (exceptionInfo == IntPtr.Zero)
            {
                return;
            }

            var type = StringUtil.PtrToStringUtf8(ExceptionInfo_Type(exceptionInfo));
            var message = StringUtil.PtrToStringUtf8(ExceptionInfo_Message(exceptionInfo));

            ExceptionInfo_Free(exceptionInfo);

            throw new ParquetException(type, message);
        }

        public static TValue Return<TValue>(GetAction<TValue> getter)
        {
            Check(getter(out var value));
            return value;
        }

        public static TValue Return<TArg0, TValue>(TArg0 arg0, GetAction<TArg0, TValue> getter)
        {
            Check(getter(arg0, out var value));
            return value;
        }

        public static TValue Return<TArg0, TArg1, TValue>(TArg0 arg0, TArg1 arg1, GetAction<TArg0, TArg1, TValue> getter)
        {
            Check(getter(arg0, arg1, out var value));
            return value;
        }

        public static TValue Return<TArg0, TArg1, TArg2, TValue>(TArg0 arg0, TArg1 arg1, TArg2 arg2, GetAction<TArg0, TArg1, TArg2, TValue> getter)
        {
            Check(getter(arg0, arg1, arg2, out var value));
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

        public static TValue Return<TArg0, TArg1, TValue>(ParquetHandle handle, TArg0 arg0, TArg1 arg1, GetFunction<TArg0, TArg1, TValue> getter)
        {
            var value = Return(handle.IntPtr, arg0, arg1, getter);
            GC.KeepAlive(handle);
            return value;
        }

        public static TValue Return<TArg0, TValue>(IntPtr handle, TArg0 arg0, GetFunction<TArg0, TValue> getter)
        {
            Check(getter(handle, arg0, out var value));
            return value;
        }

        public static TValue Return<TArg0, TArg1, TValue>(IntPtr handle, TArg0 arg0, TArg1 arg1, GetFunction<TArg0, TArg1, TValue> getter)
        {
            Check(getter(handle, arg0, arg1, out var value));
            return value;
        }

        public static string ReturnString(IntPtr handle, GetFunction<IntPtr> getter, Action<IntPtr>? deleter = null)
        {
            Check(getter(handle, out var value));
            return ConvertPtrToString(handle, deleter, value);
        }

        public static string ReturnString(ParquetHandle handle, GetFunction<IntPtr> getter, Action<IntPtr>? deleter = null)
        {
            Check(getter(handle.IntPtr, out var value));
            return ConvertPtrToString(handle, deleter, value);
        }

        private static string ConvertPtrToString(IntPtr handle, Action<IntPtr>? deleter, IntPtr value)
        {
            var str = StringUtil.PtrToStringUtf8(value);
            deleter?.Invoke(value);
            return str;
        }

        private static string ConvertPtrToString(ParquetHandle handle, Action<IntPtr>? deleter, IntPtr value)
        {
            var str = StringUtil.PtrToStringUtf8(value);
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
