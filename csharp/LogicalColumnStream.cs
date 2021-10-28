using System;
using System.Collections.Generic;

namespace ParquetSharp
{
    public abstract class LogicalColumnStream<TSource> : IDisposable
        where TSource : class, IDisposable
    {
        protected LogicalColumnStream(TSource source, ColumnDescriptor descriptor, Type elementType, Type physicalType, int bufferLength)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            ColumnDescriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
            BufferLength = bufferLength;
            LogicalType = descriptor.LogicalType;

            SchemaNodesPath = GetSchemaNode(ColumnDescriptor.SchemaNode).ToArray();

            Buffer = Array.CreateInstance(physicalType, bufferLength);
            DefLevels = descriptor.MaxDefinitionLevel == 0 ? null : new short[bufferLength];
            RepLevels = descriptor.MaxRepetitionLevel == 0 ? null : new short[bufferLength];
        }

        public virtual void Dispose()
        {
            Source.Dispose();
        }

        protected static Type? GetLeafElementType(Type? type)
        {
            while (type != null && type != typeof(byte[]) && type.IsArray)
            {
                type = type.GetElementType();
            }

            return type;
        }

        private static List<Schema.Node> GetSchemaNode(Schema.Node node)
        {
            var schemaNodes = new List<Schema.Node>();
            for (var n = node; n != null; n = n.Parent)
            {
                schemaNodes.Add(n);
            }
            schemaNodes.RemoveAt(schemaNodes.Count - 1); // we don't need the schema root
            schemaNodes.Reverse(); // root to leaf
            return schemaNodes;
        }

        public TSource Source { get; }
        public ColumnDescriptor ColumnDescriptor { get; }
        public int BufferLength { get; }
        public LogicalType LogicalType { get; }

        protected readonly Array Buffer;
        protected readonly short[]? DefLevels;
        protected readonly short[]? RepLevels;

        protected readonly Schema.Node[]? SchemaNodesPath;
    }
}
