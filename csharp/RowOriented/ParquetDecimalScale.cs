using System;

namespace ParquetSharp.RowOriented
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class ParquetDecimalScaleAttribute : Attribute
    {
        public ParquetDecimalScaleAttribute(int scale)
        {
            Scale = scale;
        }

        public readonly int Scale;
    }
}
