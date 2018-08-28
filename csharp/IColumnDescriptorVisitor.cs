
namespace ParquetSharp
{
    /// <summary>
    /// Visitor on ColumnDescriptor to discover the system types in a type safe manner.
    /// </summary>
    public interface IColumnDescriptorVisitor<out TReturn>
    {
        TReturn OnColumnDescriptor<TPhysical, TLogical, TElement>() where TPhysical : unmanaged;
    }
}
