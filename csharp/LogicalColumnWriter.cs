﻿using System;
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
                if (logicalWriterType.GetGenericTypeDefinition() != typeof(LogicalColumnWriter<>))
                {
                    throw;
                }
                var elementType = logicalWriterType.GetGenericArguments()[0];
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
                return LogicalColumnWriter<TElement>.Create<TPhysical, TLogical>(_columnWriter, _bufferLength);
            }

            private readonly ColumnWriter _columnWriter;
            private readonly int _bufferLength;
        }
    }

    public sealed class LogicalColumnWriter<TElement> : LogicalColumnWriter
    {
        private LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength, ByteBuffer? byteBuffer, ILogicalBatchWriter<TElement> batchWriter)
            : base(columnWriter, bufferLength)
        {
            _byteBuffer = byteBuffer;
            _batchWriter = batchWriter;
        }

        internal static LogicalColumnWriter<TElement> Create<TPhysical, TLogical>(ColumnWriter columnWriter, int bufferLength) where TPhysical : unmanaged
        {
            var byteBuffer = typeof(TPhysical) == typeof(ByteArray) || typeof(TPhysical) == typeof(FixedLenByteArray)
                ? new ByteBuffer(bufferLength)
                : null;

            // Convert logical values into physical values at the lowest array level
            var converter = (LogicalWrite<TLogical, TPhysical>.Converter) (
                columnWriter.LogicalWriteConverterFactory.GetConverter<TLogical, TPhysical>(columnWriter.ColumnDescriptor, byteBuffer));

            var schemaNodes = GetSchemaNodesPath(columnWriter.ColumnDescriptor.SchemaNode);
            ILogicalBatchWriter<TElement> batchWriter;
            try
            {
                var factory = new LogicalBatchWriterFactory<TPhysical, TLogical>(
                    (ColumnWriter<TPhysical>) columnWriter, byteBuffer, converter, bufferLength);
                batchWriter = factory.GetWriter<TElement>(schemaNodes);
            }
            finally
            {
                foreach (var node in schemaNodes)
                {
                    node.Dispose();
                }
            }

            return new LogicalColumnWriter<TElement>(columnWriter, bufferLength, byteBuffer, batchWriter);
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
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

        public void WriteBatch(ReadOnlySpan<TElement> values)
        {
            _batchWriter.WriteBatch(values);
        }

        private readonly ByteBuffer? _byteBuffer;
        private readonly ILogicalBatchWriter<TElement> _batchWriter;
    }
}
