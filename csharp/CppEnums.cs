using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal enum CppEnums
    {
        Binary = 14,
        LargeBinary = 35,
        BinaryView = 40,
    }

    internal static class ArrowTypeIdExtensions
    {
        public static Apache.Arrow.Types.ArrowTypeId toPublicEnum(this CppEnums binaryType) => binaryType switch
        {
            CppEnums.Binary => Apache.Arrow.Types.ArrowTypeId.Binary,
            CppEnums.LargeBinary => Apache.Arrow.Types.ArrowTypeId.LargeBinary,
            CppEnums.BinaryView => Apache.Arrow.Types.ArrowTypeId.BinaryView,
            _ => throw new ArgumentOutOfRangeException(nameof(binaryType), binaryType, null)
        };
    }
}
