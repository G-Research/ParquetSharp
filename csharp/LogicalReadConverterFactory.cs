
namespace ParquetSharp
{
    /// <summary>
    /// Extendable class that handles the mapping between a Parquet physical type and the C# logical type when reading values.
    /// </summary>
    public class LogicalReadConverterFactory
    {
        /// <summary>
        /// Return a reader delegate if a TPhysical column reader can directly write into a TLogical span (e.g. float to float, int to uint, etc).
        /// Otherwise return null.
        /// </summary>
        public virtual LogicalRead<TLogical, TPhysical>.DirectReader GetDirectReader<TLogical, TPhysical>() 
            where TPhysical : unmanaged
        {
            return LogicalRead<TLogical, TPhysical>.GetDirectReader();
        }

        /// <summary>
        /// Return a converter delegate that converts a TPhysical readonly-span to a TLogical span.
        /// </summary>
        /// <param name="columnDescriptor">The descriptor of the column to be converted.</param>
        /// <param name="columnChunkMetaData">The metadata of the column-chunk to be converted.</param>
        /// <returns></returns>
        public virtual LogicalRead<TLogical, TPhysical>.Converter GetConverter<TLogical, TPhysical>(ColumnDescriptor columnDescriptor, ColumnChunkMetaData columnChunkMetaData)
            where TPhysical : unmanaged
        {
            return LogicalRead<TLogical, TPhysical>.GetConverter(columnDescriptor, columnChunkMetaData);
        }

        public static readonly LogicalReadConverterFactory Default = new LogicalReadConverterFactory();
    }
}
