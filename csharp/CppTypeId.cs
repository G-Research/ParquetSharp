using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal enum CppTypeId
    {
        Binary = 14,
        LargeBinary = 35,
        BinaryView = 40,
        List = 25,
        LargeList = 36,
        ListView = 41
    }

    internal static class CppTypeIdExtensions
    {
        public static Apache.Arrow.Types.ArrowTypeId toPublicEnum(this CppTypeId binaryType) => binaryType switch
        {
            CppTypeId.Binary => Apache.Arrow.Types.ArrowTypeId.Binary,
            CppTypeId.LargeBinary => Apache.Arrow.Types.ArrowTypeId.LargeBinary,
            CppTypeId.BinaryView => Apache.Arrow.Types.ArrowTypeId.BinaryView,
            CppTypeId.List => Apache.Arrow.Types.ArrowTypeId.List,
            CppTypeId.LargeList => Apache.Arrow.Types.ArrowTypeId.LargeList,
            CppTypeId.ListView => Apache.Arrow.Types.ArrowTypeId.ListView,
            _ => throw new ArgumentOutOfRangeException(nameof(binaryType), binaryType, null)
        };
    }
}
