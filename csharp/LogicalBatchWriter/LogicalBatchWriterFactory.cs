using System;
using ParquetSharp.Schema;

namespace ParquetSharp.LogicalBatchWriter
{
    /// <summary>
    /// Creates batch writers for a column at different levels of the column schema hierarchy
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    internal sealed class LogicalBatchWriterFactory<TPhysical, TLogical>
        where TPhysical : unmanaged
    {
        public LogicalBatchWriterFactory(
            ColumnWriter<TPhysical> physicalWriter,
            ByteBuffer? byteBuffer,
            LogicalWrite<TLogical, TPhysical>.Converter converter,
            int bufferLength)
        {
            _physicalWriter = physicalWriter;
            _buffers = new LogicalStreamBuffers<TPhysical>(physicalWriter.ColumnDescriptor, bufferLength);
            _byteBuffer = byteBuffer;
            _converter = converter;
        }

        /// <summary>
        /// Get a writer for the top-level element type of the column
        /// </summary>
        /// <param name="schemaNodes">The full array of nodes making up the column schema</param>
        /// <typeparam name="TElement">The top-level column element type</typeparam>
        /// <returns>A batch writer for the top level element type</returns>
        public ILogicalBatchWriter<TElement> GetWriter<TElement>(Node[] schemaNodes)
        {
            return GetWriter<TElement>(schemaNodes, 0, 0, 0);
        }

        /// <summary>
        /// Get an internal element writer
        /// </summary>
        /// <param name="schemaNodes">A subset of the column schema nodes, with outer schema nodes skipped over</param>
        /// <param name="definitionLevel">The current base definition level</param>
        /// <param name="repetitionLevel">The current base repetition level</param>
        /// <param name="firstRepetitionLevel">The current repetition level for the first leaf value.
        /// This can't be inferred from the repetition level as inner level writers don't know whether they are writing values within an array</param>
        /// <typeparam name="TElement">The type of element to get a writer for</typeparam>
        private ILogicalBatchWriter<TElement> GetWriter<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            if (typeof(TElement) == typeof(TLogical))
            {
                if (schemaNodes.Length != 1)
                {
                    throw new Exception("Expected only a single schema node for the leaf element writer");
                }

                var optional = schemaNodes[0].Repetition == Repetition.Optional;
                var innerDefinitionLevel = (short) (optional ? definitionLevel + 1 : definitionLevel);
                return (
                    new ScalarWriter<TLogical, TPhysical>(
                            _physicalWriter, _buffers, _byteBuffer, _converter,
                            innerDefinitionLevel, repetitionLevel, firstRepetitionLevel, optional)
                        as ScalarWriter<TElement, TPhysical>)!;
            }

            if (TypeUtils.IsNullableNested(typeof(TElement), out var nullableNestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode {Repetition: Repetition.Optional})
                {
                    return MakeNestedOptionalWriter<TElement>(
                        nullableNestedType, schemaNodes, definitionLevel, repetitionLevel, firstRepetitionLevel);
                }
                throw new Exception("Unexpected schema for an optional nested element type");
            }

            if (TypeUtils.IsNested(typeof(TElement), out var requiredNestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode {Repetition: Repetition.Required})
                {
                    return MakeNestedWriter<TElement>(
                        requiredNestedType, schemaNodes, definitionLevel, repetitionLevel, firstRepetitionLevel);
                }
                throw new Exception("Unexpected schema for required nested element type");
            }

            // Map values are treated the same as lists,
            // as the structure of the map keys and values matches that of lists.
            if (typeof(TElement).IsArray && SchemaUtils.IsListOrMap(schemaNodes))
            {
                return MakeArrayWriter<TElement>(schemaNodes, definitionLevel, repetitionLevel, firstRepetitionLevel);
            }

            throw new Exception($"Failed to create a batch writer for type {typeof(TElement)}");
        }

        /// <summary>
        /// Create a new writer for array values
        /// </summary>
        /// <typeparam name="TElement">The type of array to write</typeparam>
        private ILogicalBatchWriter<TElement> MakeArrayWriter<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            var containedType = typeof(TElement).GetElementType() ??
                                throw new NullReferenceException(
                                    $"Element type is null for type {typeof(TElement)}, expected an array type");

            var optional = schemaNodes[0].Repetition == Repetition.Optional;
            var arrayDefinitionLevel = (short) (optional ? definitionLevel + 1 : definitionLevel);
            var elementDefinitionLevel = (short) (arrayDefinitionLevel + 1);
            var elementRepetitionLevel = (short) (repetitionLevel + 1);
            var elementSchema = schemaNodes.AsSpan().Slice(2).ToArray();

            var writer0 = MakeGenericWriter(containedType, elementSchema, elementDefinitionLevel, elementRepetitionLevel, firstRepetitionLevel);
            var writer1 = MakeGenericWriter(containedType, elementSchema, elementDefinitionLevel, elementRepetitionLevel, repetitionLevel);

            var arrayWriterType = typeof(ArrayWriter<,>).MakeGenericType(containedType, typeof(TPhysical));
            return (ILogicalBatchWriter<TElement>) Activator.CreateInstance(
                arrayWriterType, writer0, writer1, _physicalWriter, optional,
                arrayDefinitionLevel, repetitionLevel, firstRepetitionLevel);
        }

        /// <summary>
        /// Create a new writer for Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nested value to write</typeparam>
        private ILogicalBatchWriter<TElement> MakeNestedWriter<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var firstInnerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, firstRepetitionLevel);
            var innerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, repetitionLevel);

            var nestedWriterType = typeof(NestedWriter<>).MakeGenericType(nestedType);
            return (ILogicalBatchWriter<TElement>) Activator.CreateInstance(
                nestedWriterType, firstInnerWriter, innerWriter, _buffers.Length);
        }

        /// <summary>
        /// Create a new writer for optional (nullable) Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nullable nested value to write</typeparam>
        private ILogicalBatchWriter<TElement> MakeNestedOptionalWriter<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            definitionLevel += 1;
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var firstInnerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, firstRepetitionLevel);
            var innerWriter = MakeGenericWriter(nestedType, innerSchema, definitionLevel, repetitionLevel, repetitionLevel);

            var optionalNestedWriterType = typeof(OptionalNestedWriter<,>).MakeGenericType(nestedType, typeof(TPhysical));
            return (ILogicalBatchWriter<TElement>) Activator.CreateInstance(
                optionalNestedWriterType, firstInnerWriter, innerWriter, _physicalWriter, _buffers,
                definitionLevel, repetitionLevel, firstRepetitionLevel);
        }

        /// <summary>
        /// Utility method to create an ILogicalBatchWriter given the element type as a variable
        /// </summary>
        private object MakeGenericWriter(
            Type elementType,
            Node[] schemaNodes,
            short nullDefinitionLevel,
            short repetitionLevel,
            short firstRepetitionLevel)
        {
            var factoryType = typeof(LogicalBatchWriterFactory<TPhysical, TLogical>);
            var genericMethod = factoryType.GetMethod(
                nameof(GetWriter),
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (genericMethod == null)
            {
                throw new Exception($"Failed to reflect {nameof(GetWriter)} method");
            }
            return genericMethod.MakeGenericMethod(elementType).Invoke(this, new object[]
            {
                schemaNodes, nullDefinitionLevel, repetitionLevel, firstRepetitionLevel
            });
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly LogicalWrite<TLogical, TPhysical>.Converter _converter;
        private readonly ColumnWriter<TPhysical> _physicalWriter;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
    }
}
