
namespace ParquetSharp
{
    /// <summary>
    /// Visitor on ColumnReader to discover the derived reader type in a type safe manner.
    /// </summary>
    public interface IColumnReaderVisitor<out TReturn>
    {
        TReturn OnColumnReader<TValue>(ColumnReader<TValue> columnReader) where TValue : unmanaged;
    }
}
