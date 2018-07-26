
namespace ParquetSharp
{
    /// <summary>
    /// Visitor on LogicalColumnReader to discover the derived reader type in a type safe manner.
    /// </summary>
    public interface ILogicalColumnReaderVisitor<out TReturn>
    {
        TReturn OnLogicalColumnReader<TValue>(LogicalColumnReader<TValue> columnReader);
    }
}
