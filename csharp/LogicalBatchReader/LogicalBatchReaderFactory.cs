using System;
using System.Linq;
using ParquetSharp.Schema;

namespace ParquetSharp.LogicalBatchReader
{
    /// <summary>
    /// Creates batch readers for a column at different levels of the column schema hierarchy
    /// </summary>
    /// <typeparam name="TPhysical">The underlying physical type of leaf values in the column</typeparam>
    /// <typeparam name="TLogical">The .NET logical type for the column leaf values</typeparam>
    internal sealed class LogicalBatchReaderFactory<TPhysical, TLogical>
        where TPhysical : unmanaged
    {
        public LogicalBatchReaderFactory(
            ColumnReader<TPhysical> physicalReader,
            LogicalRead<TLogical, TPhysical>.DirectReader? directReader,
            LogicalRead<TLogical, TPhysical>.Converter converter,
            int bufferLength)
        {
            _physicalReader = physicalReader;
            _buffers = new LogicalStreamBuffers<TPhysical>(physicalReader.ColumnDescriptor, bufferLength);
            _converter = converter;
            _directReader = directReader;
        }

        /// <summary>
        /// Get a reader for the top-level element type of the column
        /// </summary>
        /// <param name="schemaNodes">The full array of nodes making up the column schema</param>
        /// <typeparam name="TElement">The top-level column element type</typeparam>
        /// <returns>A batch reader for the top level element type</returns>
        public ILogicalBatchReader<TElement> GetReader<TElement>(Node[] schemaNodes)
        {
            if (schemaNodes.Length == 1)
            {
                // Handle plain scalar columns
                if (typeof(TElement) != typeof(TLogical))
                {
                    throw new Exception($"Expected the element type ({typeof(TElement)}) " +
                                        $"to match the logical type ({typeof(TLogical)}) for scalar columns");
                }

                var optional = schemaNodes[0].Repetition == Repetition.Optional;
                if (_directReader != null && !optional)
                {
                    return new DirectReader<TElement, TPhysical>(
                        _physicalReader, (_directReader as LogicalRead<TElement, TPhysical>.DirectReader)!);
                }

                var definitionLevel = (short) (optional ? 1 : 0);
                return (
                    new ScalarReader<TLogical, TPhysical>(_physicalReader, _converter, _buffers, definitionLevel)
                        as ScalarReader<TElement, TPhysical>)!;
            }

            // Otherwise we have a more complex structure and use a buffered reader
            var leafDefinitionLevel = (short) schemaNodes.Count(n => n.Repetition != Repetition.Required);
            // Reference typed leaf values are always treated as nullable as the converters are created based on the
            // .NET type and don't consider the schema nullability.
            var nullableLeafValues = schemaNodes.Last().Repetition == Repetition.Optional || !typeof(TLogical).IsValueType;
            _bufferedReader = new BufferedReader<TLogical, TPhysical>(
                _physicalReader, _converter, _buffers.Values, _buffers.DefLevels, _buffers.RepLevels, leafDefinitionLevel, nullableLeafValues);
            return GetCompoundReader<TElement>(schemaNodes, 0, 0);
        }

        /// <summary>
        /// Get an internal element reader
        /// </summary>
        /// <param name="schemaNodes">A subset of the column schema nodes, with outer schema nodes skipped over</param>
        /// <param name="definitionLevel">The current base definition level</param>
        /// <param name="repetitionLevel">The current base repetition level</param>
        /// <typeparam name="TElement">The type of element to get a reader for</typeparam>
        private ILogicalBatchReader<TElement> GetCompoundReader<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            if (_bufferedReader == null)
            {
                throw new Exception("A buffered reader is required for reading compound column values");
            }

            if (TypeUtils.IsNullableNested(typeof(TElement), out var nullableNestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode { Repetition: Repetition.Optional })
                {
                    return MakeNestedOptionalReader<TElement>(
                        nullableNestedType, schemaNodes, definitionLevel, repetitionLevel);
                }
                throw new Exception("Unexpected schema for an optional nested element type");
            }

            if (TypeUtils.IsNested(typeof(TElement), out var requiredNestedType))
            {
                if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode { Repetition: Repetition.Required })
                {
                    return MakeNestedReader<TElement>(
                        requiredNestedType, schemaNodes, definitionLevel, repetitionLevel);
                }
                throw new Exception("Unexpected schema for required nested element type");
            }

            // Map values are treated the same as lists,
            // as the structure of the map keys and values matches that of lists.
            if (typeof(TElement).IsArray && SchemaUtils.IsListOrMap(schemaNodes))
            {
                return MakeArrayReader<TElement>(schemaNodes, definitionLevel, repetitionLevel);
            }

            if (schemaNodes.Length > 1 && schemaNodes[0] is GroupNode groupNode)
            {
                // Schema uses a group, but doesn't represent a list or map and
                // the desired data type doesn't use the Nested type wrapper,
                // so skip over this level of the type hierarchy and create a reader for the inner type.
                var optional = groupNode.Repetition == Repetition.Optional;
                var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
                var innerDefinitionLevel = (short) (definitionLevel + (optional ? 1 : 0));
                return (ILogicalBatchReader<TElement>) MakeGenericReader(typeof(TElement), innerSchema, innerDefinitionLevel, repetitionLevel);
            }

            if (typeof(TElement) == typeof(TLogical))
            {
                if (schemaNodes.Length != 1)
                {
                    throw new Exception("Expected only a single schema node for the leaf element reader");
                }

                return (new LeafReader<TLogical, TPhysical>(_bufferedReader) as LeafReader<TElement, TPhysical>)!;
            }

            // If not using nesting, we may need to read nullable values that have a non-nullable logical type
            // due to required values being wrapped in an optional group.
            if (TypeUtils.IsNullable(typeof(TElement), out var innerNullableType) && innerNullableType == typeof(TLogical))
            {
                if (schemaNodes.Length != 1)
                {
                    throw new Exception("Expected only a single schema node for the leaf element reader");
                }

                return MakeOptionalReader<TElement>(
                    innerNullableType, schemaNodes, definitionLevel, repetitionLevel);
            }

            throw new Exception($"Failed to create a batch reader for type {typeof(TElement)}");
        }

        /// <summary>
        /// Create a new reader for array values
        /// </summary>
        /// <typeparam name="TElement">The type of array to read</typeparam>
        private ILogicalBatchReader<TElement> MakeArrayReader<TElement>(
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            var containedType = typeof(TElement).GetElementType() ??
                                throw new NullReferenceException(
                                    $"Element type is null for type {typeof(TElement)}, expected an array type");

            var optional = schemaNodes[0].Repetition == Repetition.Optional;
            var arrayDefinitionLevel = (short) (optional ? definitionLevel + 1 : definitionLevel);
            var innerDefinitionLevel = (short) (arrayDefinitionLevel + 1);
            var innerRepetitionLevel = (short) (repetitionLevel + 1);
            var innerSchema = schemaNodes.AsSpan().Slice(2).ToArray();

            var innerNodeIsOptional = innerSchema[0].Repetition == Repetition.Optional;
            var innerReader = MakeGenericReader(
                containedType, innerSchema, innerDefinitionLevel, innerRepetitionLevel);

            var arrayReaderType = typeof(ArrayReader<,,>).MakeGenericType(typeof(TPhysical), typeof(TLogical), containedType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(
                arrayReaderType, innerReader, _bufferedReader!, arrayDefinitionLevel, repetitionLevel, innerNodeIsOptional)!;
        }

        /// <summary>
        /// Create a new reader for Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nested value to read</typeparam>
        private ILogicalBatchReader<TElement> MakeNestedReader<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var innerReader = MakeGenericReader(nestedType, innerSchema, definitionLevel, repetitionLevel);

            var nestedReaderType = typeof(NestedReader<>).MakeGenericType(nestedType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(nestedReaderType, innerReader, _buffers.Length)!;
        }

        /// <summary>
        /// Create a new reader for optional (nullable) Nested values
        /// </summary>
        /// <typeparam name="TElement">The type of nullable nested value to read</typeparam>
        private ILogicalBatchReader<TElement> MakeNestedOptionalReader<TElement>(
            Type nestedType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            definitionLevel += 1;
            var innerSchema = schemaNodes.AsSpan().Slice(1).ToArray();
            var innerReader = MakeGenericReader(nestedType, innerSchema, definitionLevel, repetitionLevel);

            var optionalNestedReaderType = typeof(OptionalNestedReader<,,>).MakeGenericType(
                typeof(TPhysical), typeof(TLogical), nestedType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(
                optionalNestedReaderType, innerReader, _bufferedReader!, definitionLevel)!;
        }

        /// <summary>
        /// Create a new reader for required leaf level values that are nullable due to nesting,
        /// but don't use the Nested wrapper type.
        /// </summary>
        /// <typeparam name="TElement">The type of nullable value to read</typeparam>
        private ILogicalBatchReader<TElement> MakeOptionalReader<TElement>(
            Type innerType,
            Node[] schemaNodes,
            short definitionLevel,
            short repetitionLevel)
        {
            var innerReader = MakeGenericReader(innerType, schemaNodes, definitionLevel, repetitionLevel);
            var optionalReaderType = typeof(OptionalReader<,,>).MakeGenericType(
                typeof(TPhysical), typeof(TLogical), innerType);
            return (ILogicalBatchReader<TElement>) Activator.CreateInstance(
                optionalReaderType, innerReader, _bufferedReader!, definitionLevel)!;
        }

        /// <summary>
        /// Utility method to create an ILogicalBatchReader given the element type as a variable
        /// </summary>
        private object MakeGenericReader(
            Type elementType,
            Node[] schemaNodes,
            short nullDefinitionLevel,
            short repetitionLevel)
        {
            var factoryType = typeof(LogicalBatchReaderFactory<TPhysical, TLogical>);
            var genericMethod = factoryType.GetMethod(
                nameof(GetCompoundReader),
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (genericMethod == null)
            {
                throw new Exception($"Failed to reflect {nameof(GetCompoundReader)} method");
            }
            return genericMethod.MakeGenericMethod(elementType).Invoke(this, new object[]
            {
                schemaNodes, nullDefinitionLevel, repetitionLevel
            })!;
        }

        private readonly ColumnReader<TPhysical> _physicalReader;
        private readonly LogicalStreamBuffers<TPhysical> _buffers;
        private BufferedReader<TLogical, TPhysical>? _bufferedReader;
        private readonly LogicalRead<TLogical, TPhysical>.DirectReader? _directReader;
        private readonly LogicalRead<TLogical, TPhysical>.Converter _converter;
    }
}
