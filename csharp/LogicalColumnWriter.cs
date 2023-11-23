using System;
using ParquetSharp.Schema;
using ParquetSharp.LogicalBatchWriter;

namespace ParquetSharp
{
    /// <summary>
    /// Column writer transparently converting C# types to Parquet physical types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnWriter : LogicalColumnStream<ColumnWriter>
    {
        protected LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, columnWriter.ColumnDescriptor, bufferLength)
        {
        }

        internal static LogicalColumnWriter Create(ColumnWriter columnWriter, int bufferLength, Type? elementTypeOverride)
        {
            if (columnWriter == null) throw new ArgumentNullException(nameof(columnWriter));

            // If the file writer was constructed with a Columns[] argument, or if an elementTypeOverride is given,
            // then we already know what the column writer logical system type should be.
            var columns = columnWriter.RowGroupWriter.ParquetFileWriter.Columns;
            var columnLogicalTypeOverride = GetLeafElementType(elementTypeOverride ?? columns?[columnWriter.ColumnIndex].LogicalSystemType);
            // Nested types must be used if writing data with a nested structure
            const bool useNesting = true;

            return columnWriter.ColumnDescriptor.Apply(
                columnWriter.LogicalTypeFactory,
                columnLogicalTypeOverride,
                useNesting,
                new Creator(columnWriter, bufferLength));
        }

        internal static LogicalColumnWriter<TElementType> Create<TElementType>(ColumnWriter columnWriter, int bufferLength, Type? elementTypeOverride)
        {
            var writer = Create(columnWriter, bufferLength, elementTypeOverride);

            try
            {
                return (LogicalColumnWriter<TElementType>) writer;
            }
            catch (InvalidCastException exception)
            {
                var logicalWriterType = writer.GetType();
                var colName = columnWriter.ColumnDescriptor.Name;
                writer.Dispose();
                if (logicalWriterType.GetGenericTypeDefinition() != typeof(LogicalColumnWriter<,,>))
                {
                    throw;
                }
                var elementType = logicalWriterType.GetGenericArguments()[2];
                var expectedElementType = typeof(TElementType);
                var message =
                    $"Tried to get a LogicalColumnWriter for column {columnWriter.ColumnIndex} ('{colName}') " +
                    $"with an element type of '{expectedElementType}' " +
                    $"but the actual element type is '{elementType}'.";
                throw new InvalidCastException(message, exception);
            }
            catch
            {
                writer.Dispose();
                throw;
            }
        }

        public abstract TReturn Apply<TReturn>(ILogicalColumnWriterVisitor<TReturn> visitor);

        private sealed class Creator : IColumnDescriptorVisitor<LogicalColumnWriter>
        {
            public Creator(ColumnWriter columnWriter, int bufferLength)
            {
                _columnWriter = columnWriter;
                _bufferLength = bufferLength;
            }

            public LogicalColumnWriter OnColumnDescriptor<TPhysical, TLogical, TElement>() where TPhysical : unmanaged
            {
                return new LogicalColumnWriter<TPhysical, TLogical, TElement>(_columnWriter, _bufferLength);
            }

            private readonly ColumnWriter _columnWriter;
            private readonly int _bufferLength;
        }
    }

    public abstract class LogicalColumnWriter<TElement> : LogicalColumnWriter
    {
        protected LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, bufferLength)
        {
        }

        public override TReturn Apply<TReturn>(ILogicalColumnWriterVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnWriter(this);
        }

        public void WriteBatch(TElement[] values)
        {
            WriteBatch(values.AsSpan());
        }

        public void WriteBatch(TElement[] values, int start, int length)
        {
            WriteBatch(values.AsSpan(start, length));
        }

        public abstract void WriteBatch(ReadOnlySpan<TElement> values);
    }

    internal sealed class LogicalColumnWriter<TPhysical, TLogical, TElement> : LogicalColumnWriter<TElement>
        where TPhysical : unmanaged
    {
        internal LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, bufferLength)
        {
            _byteBuffer = typeof(TPhysical) == typeof(ByteArray) || typeof(TPhysical) == typeof(FixedLenByteArray)
                ? new ByteBuffer(bufferLength)
                : null;

            // Convert logical values into physical values at the lowest array level
            var converter = (LogicalWrite<TLogical, TPhysical>.Converter) (
                columnWriter.LogicalWriteConverterFactory.GetConverter<TLogical, TPhysical>(ColumnDescriptor, _byteBuffer));

            var schemaNodes = GetSchemaNodesPath(ColumnDescriptor.SchemaNode);
            try
            {
                var buffer = new LogicalColumnStreamBuffer(ColumnDescriptor, typeof(TPhysical), bufferLength);
                var factory = new LogicalBatchWriterFactory<TPhysical, TLogical>(
                    (ColumnWriter<TPhysical>) Source, (TPhysical[]) buffer.Buffer, buffer.DefLevels, buffer.RepLevels, _byteBuffer, converter);
                _batchWriter = factory.GetWriter<TElement>(schemaNodes);
            }
            finally
            {
                foreach (var node in schemaNodes)
                {
                    node.Dispose();
                }
            }
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
        }

        public override void WriteBatch(ReadOnlySpan<TElement> values)
        {
            _batchWriter.WriteBatch(values);
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly ILogicalBatchWriter<TElement> _batchWriter;
    }
}
