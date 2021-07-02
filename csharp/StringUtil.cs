using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal static class StringUtil
    {
        public static unsafe IntPtr ToCStringUtf8(string str, ByteBuffer byteBuffer)
        {
            var utf8 = System.Text.Encoding.UTF8;
            var byteCount = utf8.GetByteCount(str);
            var byteArray = byteBuffer.Allocate(byteCount + 1);

            fixed (char* chars = str)
            {
                utf8.GetBytes(chars, str.Length, (byte*) byteArray.Pointer, byteCount);
            }

            return byteArray.Pointer;
        }

        public static string PtrToStringUtf8(IntPtr ptr)
        {
            return PtrToNullableStringUtf8(ptr) ?? throw new ArgumentNullException(nameof(ptr));
        }

        public static string? PtrToNullableStringUtf8(IntPtr ptr)
        {
#if NETSTANDARD2_1_OR_GREATER
            return Marshal.PtrToStringUTF8(ptr);
#else
            if (ptr == IntPtr.Zero)
            {
                return null;
            }

            unsafe
            {
                var s = (byte*) ptr;
                int length;
                for (length = 0; s[length] != '\0'; ++length)
                {
                }

                return System.Text.Encoding.UTF8.GetString(s, length);
            }
#endif
        }
    }
}
