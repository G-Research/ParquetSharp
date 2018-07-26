
namespace ParquetSharp
{
    /// <summary>
    /// Visitor on LogicalColumnWriter to discover the derived writer type in a type safe manner.
    /// </summary>
    public interface ILogicalColumnWriterVisitor<out TReturn>
    {
        TReturn OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter);
    }
}
