
using System;

namespace ParquetSharp
{
    /// <summary>
    /// Extendable class that handles the mapping between a Parquet physical type and the C# logical type when reading values.
    /// </summary>
    public class LogicalReadConverterFactory
    {
        /// <summary>
        /// Return a reader delegate if a TPhysical column reader can directly write into a TLogical span (e.g. float to float, int to uint, etc).
        /// Otherwise return null. This is an optimisation to avoid needless memory copies between buffers (i.e. otherwise we have to use the
        /// identity converter).
        /// </summary>
        public virtual Delegate GetDirectReader<TLogical, TPhysical>() 
            where TPhysical : unmanaged
        {
            return LogicalRead<TLogical, TPhysical>.GetDirectReader();
        }

        /// <summary>
        /// Return a converter delegate that converts a TPhysical readonly-span to a TLogical span.
        /// </summary>
        /// <param name="columnDescriptor">The descriptor of the column to be converted.</param>
        /// <param name="columnChunkMetaData">The metadata of the column-chunk to be converted.</param>
        public virtual Delegate GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            where TPhysical : unmanaged
        {
            return LogicalRead<TLogical, TPhysical>.GetConverter(columnDescriptor, columnChunkMetaData);
        }

        public static readonly LogicalReadConverterFactory Default = new();
    }
}
