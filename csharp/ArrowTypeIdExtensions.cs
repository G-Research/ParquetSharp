using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal static class ArrowTypeIdExtensions
    {
        public static ParquetSharp.CppTypeId toCppEnum(this Apache.Arrow.Types.ArrowTypeId arrowTypeId) => arrowTypeId switch
        {
            Apache.Arrow.Types.ArrowTypeId.Binary => ParquetSharp.CppTypeId.Binary,
            Apache.Arrow.Types.ArrowTypeId.LargeBinary => ParquetSharp.CppTypeId.LargeBinary,
            Apache.Arrow.Types.ArrowTypeId.BinaryView => ParquetSharp.CppTypeId.BinaryView,
            Apache.Arrow.Types.ArrowTypeId.List => ParquetSharp.CppTypeId.List,
            Apache.Arrow.Types.ArrowTypeId.LargeList => ParquetSharp.CppTypeId.LargeList,
            Apache.Arrow.Types.ArrowTypeId.ListView => ParquetSharp.CppTypeId.ListView,
            _ => throw new ArgumentOutOfRangeException(nameof(arrowTypeId), arrowTypeId, null)
        };
    }
}
