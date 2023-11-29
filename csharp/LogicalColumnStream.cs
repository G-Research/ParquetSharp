using System;
using System.Collections.Generic;

namespace ParquetSharp
{
    public abstract class LogicalColumnStream<TSource> : IDisposable
        where TSource : class, IDisposable
    {
        protected LogicalColumnStream(TSource source, ColumnDescriptor descriptor, int bufferLength)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            ColumnDescriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
            BufferLength = bufferLength;
            LogicalType = descriptor.LogicalType;
        }

        public virtual void Dispose()
        {
            Source.Dispose();
        }

        protected static Type? GetLeafElementType(Type? type)
        {
            while (type != null)
            {
                if (type != typeof(byte[]) && type.IsArray)
                {
                    type = type.GetElementType()!;
                }
                else if (TypeUtils.IsNested(type, out var nestedType))
                {
                    type = nestedType;
                }
                else if (TypeUtils.IsNullableNested(type, out var nullableNestedType))
                {
                    type = nullableNestedType;
                }
                else
                {
                    break;
                }
            }

            return type;
        }

        /// <summary>
        /// Get the path from the top-level schema column node to the leaf node for this column,
        /// excluding the schema root.
        /// The returned nodes should be disposed of when finished with.
        /// </summary>
        protected static Schema.Node[] GetSchemaNodesPath(Schema.Node node)
        {
            var schemaNodes = new List<Schema.Node>();
            for (var n = node; n != null; n = n.Parent)
            {
                schemaNodes.Add(n);
            }
            schemaNodes[schemaNodes.Count - 1].Dispose();
            schemaNodes.RemoveAt(schemaNodes.Count - 1); // we don't need the schema root
            schemaNodes.Reverse(); // root to leaf
            return schemaNodes.ToArray();
        }

        public TSource Source { get; }
        public ColumnDescriptor ColumnDescriptor { get; }
        public int BufferLength { get; }
        public LogicalType LogicalType { get; }
    }
}
