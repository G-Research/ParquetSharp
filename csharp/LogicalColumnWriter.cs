using System;

namespace ParquetSharp
{
    /// <summary>
    /// Column writer transparently converting C# types to Parquet physical types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnWriter : LogicalColumnStream<ColumnWriter>
    {
        protected LogicalColumnWriter(ColumnWriter columnWriter, Type elementType, int bufferLength)
            : base(columnWriter, columnWriter.ColumnDescriptor, elementType, columnWriter.ElementType, bufferLength)
        {
        }

        internal static LogicalColumnWriter Create(ColumnWriter columnWriter, int bufferLength = 4 * 1024)
        {
            if (columnWriter == null) throw new ArgumentNullException(nameof(columnWriter));

            return columnWriter.ColumnDescriptor.Apply(new Creator(columnWriter, bufferLength));
        }

        internal static LogicalColumnWriter<TElementType> Create<TElementType>(ColumnWriter columnWriter, int bufferLength = 4 * 1024)
        {
            var writer = Create(columnWriter, bufferLength);

            try
            {
                return (LogicalColumnWriter<TElementType>) writer;
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
            : base(columnWriter, typeof(TElement), bufferLength)
        {
        }

        public override TReturn Apply<TReturn>(ILogicalColumnWriterVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnWriter(this);
        }

        public void WriteBatch(TElement[] values)
        {
            WriteBatch(values, 0, values.Length);
        }

        public abstract void WriteBatch(TElement[] values, int start, int length);
    }

    internal sealed class LogicalColumnWriter<TPhysical, TLogical, TElement> : LogicalColumnWriter<TElement>
        where TPhysical : unmanaged
    {
        internal LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, bufferLength)
        {
            _byteBuffer = typeof(TPhysical) == typeof(ByteArray) ? new ByteBuffer(bufferLength) : null;
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
        }

        public override void WriteBatch(TElement[] values, int start, int length)
        {
            // Convert logical values into physical values at the lowest array level
            var converter = LogicalWrite<TLogical, TPhysical>.GetConverter(LogicalType, _byteBuffer);

            // Handle arrays separately
            if (typeof(TElement) != typeof(byte[]) && typeof(TElement).IsArray)
            {
                WriteBatchArray(values, start, length, converter);
                return;
            }

            WriteBatchSimple(((TLogical[]) (object) values).AsSpan(start, length), converter);
        }

        private void WriteBatchArray(TElement[] values, int start, int length, LogicalWrite<TLogical, TPhysical>.Converter converter)
        {
            WriteArrayInternal(values, start, length, 0, NestingDepth, 0, NullDefinitionLevels, ColumnDescriptor.MaxDefinitionLevel, converter);
        }

        private void WriteArrayInternal(
            Array values, int start, int length,
            short repetitionLevel, short maxRepetitionLevel, short leafFirstRepLevel, 
            short[] nullDefinitionLevels, short leafDefinitionLevel,
            LogicalWrite<TLogical, TPhysical>.Converter converter)
        {
            if (values.Length < start + length) throw new ArgumentOutOfRangeException(nameof(length), "start + length is larger tha values length");

            short nullDefinitionLevel = nullDefinitionLevels[repetitionLevel];
            bool writtenFirstItem = false;

            if (repetitionLevel == maxRepetitionLevel)
            {
                if (nullDefinitionLevel != -1 && DefLevels == null)
                {
                    throw new Exception("Internal error: DefLevels should not be null.");
                }

                var rowsWritten = 0;
                var columnWriter = (ColumnWriter<TPhysical>) Source;
                var buffer = (TPhysical[]) Buffer;

                while (rowsWritten < length)
                {
                    var bufferLength = Math.Min(length - rowsWritten, buffer.Length);

                    converter(((TLogical[]) values).AsSpan(start + rowsWritten), DefLevels, buffer, nullDefinitionLevel);

                    for (int i = 0; i < bufferLength; i++)
                    {
                        RepLevels[i] = repetitionLevel;

                        // If the leaves are required, we have to write the deflevel because the converter won't do this for us.
                        if (nullDefinitionLevel == -1)
                        {
                            DefLevels[i] = leafDefinitionLevel;
                        }
                    }

                    if (!writtenFirstItem)
                    {
                        RepLevels[0] = leafFirstRepLevel;
                    }

                    columnWriter.WriteBatch(bufferLength, DefLevels, RepLevels, buffer);
                    rowsWritten += bufferLength;

                    writtenFirstItem = true;

                    _byteBuffer?.Clear();
                }

                return;
            }

            for (int i = start; i != start + length; ++i)
            {
                var item = (Array) values.GetValue(i);
                short innerLeafFirstRepLevel = writtenFirstItem ? repetitionLevel : leafFirstRepLevel;

                if (item != null)
                {
                    WriteArrayInternal(
                        item, 0, item.Length, 
                        (short) (repetitionLevel + 1), maxRepetitionLevel, innerLeafFirstRepLevel,
                        nullDefinitionLevels, leafDefinitionLevel, converter);
                }
                else
                {
                    if (nullDefinitionLevel == -1)
                    {
                        throw new Exception("Array is null but the schema says it is a required value.");
                    }

                    var columnWriter = (ColumnWriter<TPhysical>) Source;

                    columnWriter.WriteBatchSpaced(
                        1, new[] {nullDefinitionLevel}, new[] {innerLeafFirstRepLevel}, 
                        new byte[] {0}, 0, new TPhysical[] { });
                }

                writtenFirstItem = true;
            }
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private void WriteBatchSimple(ReadOnlySpan<TLogical> values, LogicalWrite<TLogical, TPhysical>.Converter converter)
        {
            var rowsWritten = 0;
            var nullLevel = DefLevels == null ? (short) -1 : (short) 0;
            var columnWriter = (ColumnWriter<TPhysical>) Source;
            var buffer = (TPhysical[]) Buffer;

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, buffer.Length);

                converter(values.Slice(rowsWritten, bufferLength), DefLevels, buffer, nullLevel);
                columnWriter.WriteBatch(bufferLength, DefLevels, RepLevels, buffer);
                rowsWritten += bufferLength;

                _byteBuffer?.Clear();
            }
        }

        private readonly ByteBuffer _byteBuffer;
    }
}
