
namespace ParquetSharp
{
    /// <summary>
    /// Visitor on ColumnWriter to discover the derived writer type in a type safe manner.
    /// </summary>
    public interface IColumnWriterVisitor<out TReturn>
    {
        TReturn OnColumnWriter<TValue>(ColumnWriter<TValue> columnWriter) where TValue : unmanaged;
    }
}
