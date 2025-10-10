using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal static class BinaryTypeExtensions
    {
        public static ParquetSharp.CppEnums toCppEnum(this Apache.Arrow.Types.ArrowTypeId arrowTypeId) => arrowTypeId switch
        {
            Apache.Arrow.Types.ArrowTypeId.Binary => ParquetSharp.CppEnums.Binary,
            Apache.Arrow.Types.ArrowTypeId.LargeBinary => ParquetSharp.CppEnums.LargeBinary,
            Apache.Arrow.Types.ArrowTypeId.BinaryView => ParquetSharp.CppEnums.BinaryView,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowTypeId), arrowTypeId, null)
        };
    }
}
