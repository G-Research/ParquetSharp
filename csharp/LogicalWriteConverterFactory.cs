
using System;

namespace ParquetSharp
{
    /// <summary>
    /// Extendable class that handles the mapping between a C# logical type and the Parquet physical type when writing values.
    /// </summary>
    public class LogicalWriteConverterFactory
    {
        // tanguyf: 2020-04-15: there is no GetDirectWriter delegate like with LogicalReadConverterFactory.
        // While this would nicely mirror LogicalReadConverterFactory interface, it is actually not needed in practice
        // since Parquet column writing is much slower than reading. Hence there is limited value for such an optimisation in this case,
        // the overhead of needlessly copying the memory is dwarfed by everything else.

        /// <summary>
        /// Return a converter delegate that converts a TLogical readonly-span to a TPhysical span.
        /// </summary>
        /// <param name="columnDescriptor">The descriptor of the column to be converted.</param>
        /// <param name="byteBuffer">The ByteBuffer allocation pool for efficiently handling byte arrays.</param>
        public virtual Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ByteBuffer byteBuffer)
            where TPhysical : unmanaged
        {
            return LogicalWrite<TLogical, TPhysical>.GetConverter(columnDescriptor, byteBuffer);
        }

        public static readonly LogicalWriteConverterFactory Default = new();
    }
}
