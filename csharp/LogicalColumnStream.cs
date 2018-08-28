using System;
using System.Collections.Generic;
using System.Linq;

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

            if (elementType != typeof(byte[]) && elementType.IsArray)
            {
                (NestingDepth, NullDefinitionLevels) = GetArraySchemaInfo(elementType, ColumnDescriptor.SchemaNode, ColumnDescriptor.MaxRepetitionLevel);
            }

            Buffer = Array.CreateInstance(physicalType, bufferLength);
            DefLevels = descriptor.MaxDefinitionLevel == 0 ? null : new short[bufferLength];
            RepLevels = descriptor.MaxRepetitionLevel == 0 ? null : new short[bufferLength];
        }

        public virtual void Dispose()
        {
            Source.Dispose();
        }

        public TSource Source { get; }
        public ColumnDescriptor ColumnDescriptor { get; }
        public int BufferLength { get; }
        public LogicalType LogicalType { get; }

        protected readonly short NestingDepth;
        protected readonly short[] NullDefinitionLevels;

        protected readonly Array Buffer;
        protected readonly short[] DefLevels;
        protected readonly short[] RepLevels;

        private static (short nestingDepth, short[] nullDefinitionLevels) GetArraySchemaInfo(Type elementType, Schema.Node node, int maxRepetitionLevel)
        {
            var schemaNodes = new List<Schema.Node>();
            for (; node != null; node = node.Parent)
            {
                schemaNodes.Add(node);
            }
            schemaNodes.RemoveAt(schemaNodes.Count - 1); // we don't need the schema root
            schemaNodes.Reverse(); // root to leaf

            var nestingDepth = schemaNodes.Count(n => n.LogicalType == LogicalType.List);
            var nullDefinitionLevels = new short[nestingDepth + 1];
            int nestingLevel = 0;

            // By default mark every level as required.
            for (int i = 0; i < nullDefinitionLevels.Length; i++)
            {
                nullDefinitionLevels[i] = -1;
            }

            for (int i = 0; i < schemaNodes.Count; i++)
            {
                if (schemaNodes[i].Repetition == Repetition.Optional)
                {
                    nullDefinitionLevels[nestingLevel++] = (short)i;
                }
            }

            // Check the type matches
            var maxRepLevel = maxRepetitionLevel;
            var depth = 0;

            for (var type = elementType; type != typeof(byte[]) && type.IsArray; type = type.GetElementType())
            {
                depth++;
            }

            if (nestingDepth != depth || depth != maxRepLevel)
            {
                throw new Exception("Schema does not match type we are trying to read into.");
            }

            return (checked((short) nestingDepth), nullDefinitionLevels);
        }
    }
}
