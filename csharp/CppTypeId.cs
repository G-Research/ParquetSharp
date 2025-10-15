using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal enum CppTypeId
    {
        Binary = 14,
        LargeBinary = 35,
        BinaryView = 40,
    }

    internal static class CppTypeIdExtensions
    {
        public static Apache.Arrow.Types.ArrowTypeId toPublicEnum(this CppTypeId binaryType) => binaryType switch
        {
            CppTypeId.Binary => Apache.Arrow.Types.ArrowTypeId.Binary,
            CppTypeId.LargeBinary => Apache.Arrow.Types.ArrowTypeId.LargeBinary,
            CppTypeId.BinaryView => Apache.Arrow.Types.ArrowTypeId.BinaryView,
            _ => throw new ArgumentOutOfRangeException(nameof(binaryType), binaryType, null)
        };
    }
}
