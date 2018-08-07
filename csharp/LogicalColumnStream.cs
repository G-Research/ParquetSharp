using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharp
{
    public abstract class LogicalColumnStream<TDerived, TSource> : IDisposable
        where TSource : class, IDisposable
    {
        protected LogicalColumnStream(TSource source, ColumnDescriptor descriptor, Type elementType, Type physicalType, int bufferLength)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            ColumnDescriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
            BufferLength = bufferLength;

            if (elementType != typeof(byte[]) && elementType.IsArray)
            {
                (NestingDepth, NullDefinitionLevels) = GetArraySchemaInfo(elementType, ColumnDescriptor.SchemaNode, ColumnDescriptor.MaxRepetitionLevel);
            }

            Buffer = Array.CreateInstance(physicalType, bufferLength);
            DefLevels = descriptor.MaxDefinitionlevel == 0 ? null : new short[bufferLength];
            RepLevels = descriptor.MaxRepetitionLevel == 0 ? null : new short[bufferLength];
        }

        public virtual void Dispose()
        {
            Source.Dispose();
        }

        public TSource Source { get; }
        public ColumnDescriptor ColumnDescriptor { get; }
        public int BufferLength { get; }

        protected readonly short NestingDepth;
        protected readonly short[] NullDefinitionLevels;

        protected readonly Array Buffer;
        protected readonly short[] DefLevels;
        protected readonly short[] RepLevels;

        protected static TDerived Create(Type genericTypeDefinition, ColumnDescriptor descriptor, TSource source, int bufferLength)
        {
            var creator = CreatorCache.GetOrAdd(GetGenericArguments(descriptor), t =>
            {
                const BindingFlags bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance;

                var sourceParam = Expression.Parameter(typeof(TSource), nameof(source));
                var bufferLengthParam = Expression.Parameter(typeof(int), nameof(bufferLength));
                var args = new[] {sourceParam.Type, bufferLengthParam.Type};

                var columnWriterType = genericTypeDefinition.MakeGenericType(t.physicalType, t.logicalType, t.elementType);
                var ctor = columnWriterType.GetConstructor(bindingFlags, null, args, null);
                var newExpr = Expression.New(ctor, sourceParam, bufferLengthParam);

                return Expression.Lambda<CreateMethod>(newExpr, sourceParam, bufferLengthParam).Compile();
            });

            return creator(source, bufferLength);
        }

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

        private static (Type physicalType, Type logicalType, Type elementType) GetGenericArguments(ColumnDescriptor descriptor)
        {
            var (physicalType, logicalType) = GetColumnPhysicalAndLogicalTypes(descriptor);
            var elementType = logicalType;

            for (var node = descriptor.SchemaNode; node != null; node = node.Parent)
            {
                if (node.LogicalType == LogicalType.List)
                {
                    elementType = elementType.MakeArrayType();
                }
            }

            return (physicalType, logicalType, elementType);
        }

        private static (Type physicalType, Type logicalType) GetColumnPhysicalAndLogicalTypes(ColumnDescriptor descriptor)
        {
            var physicalType = descriptor.PhysicalType;
            var logicalType = descriptor.LogicalType;
            var nullable = descriptor.SchemaNode.Repetition == Repetition.Optional;

            switch (logicalType)
            {
                case LogicalType.None:

                    switch (physicalType)
                    {
                        case ParquetType.Boolean:
                            return (typeof(bool), nullable ? typeof(bool?) : typeof(bool));
                        case ParquetType.Int32:
                            return (typeof(int), nullable ? typeof(int?) : typeof(int));
                        case ParquetType.Int64:
                            return (typeof(long), nullable ? typeof(long?) : typeof(long));
                        case ParquetType.Int96:
                            return (typeof(Int96), nullable ? typeof(Int96?) : typeof(Int96));
                        case ParquetType.Float:
                            return (typeof(float), nullable ? typeof(float?) : typeof(float));
                        case ParquetType.Double:
                            return (typeof(double), nullable ? typeof(double?) : typeof(double));
                        case ParquetType.ByteArray:
                            return (typeof(ByteArray), typeof(byte[]));
                    }

                    break;

                case LogicalType.Int32:
                    return (typeof(int), nullable ? typeof(int?) : typeof(int));

                case LogicalType.UInt32:
                    return (typeof(int), nullable ? typeof(uint?) : typeof(uint));

                case LogicalType.Int64:
                    return (typeof(long), nullable ? typeof(long?) : typeof(long));

                case LogicalType.UInt64:
                    return (typeof(long), nullable ? typeof(ulong?) : typeof(ulong));

                case LogicalType.Date:
                    return (typeof(int), nullable ? typeof(Date?) : typeof(Date));

                case LogicalType.TimestampMicros:
                    return (typeof(long), nullable ? typeof(DateTime?) : typeof(DateTime));

                case LogicalType.TimeMicros:
                    return (typeof(long), nullable ? typeof(TimeSpan?) : typeof(TimeSpan));

                case LogicalType.Utf8:
                    return (typeof(ByteArray), typeof(string));
            }

            throw new ArgumentOutOfRangeException(nameof(logicalType), $"unsupported logical type {logicalType} with physical type {physicalType}");
        }

        private delegate TDerived CreateMethod(TSource columnWriter, int bufferLength);

        private static readonly ConcurrentDictionary<(Type physicalType, Type logicalType, Type elementType), CreateMethod> CreatorCache = 
            new ConcurrentDictionary<(Type physicalType, Type logicalType, Type elementType), CreateMethod>();
    }
}
