using System;

namespace ParquetSharp
{
    /// <summary>
    /// Column writer transparently converting C# types to Parquet physical types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnWriter : LogicalColumnStream<LogicalColumnWriter, ColumnWriter>
    {
        protected LogicalColumnWriter(ColumnWriter columnWriter, Type elementType, int bufferLength)
            : base(columnWriter, columnWriter.ColumnDescriptor, elementType, columnWriter.ElementType, bufferLength)
        {
        }

        internal static LogicalColumnWriter Create(ColumnWriter columnWriter, int bufferLength = 4 * 1024)
        {
            if (columnWriter == null) throw new ArgumentNullException(nameof(columnWriter));

            return Create(typeof(LogicalColumnWriter<,,>), columnWriter.ColumnDescriptor, columnWriter, bufferLength);
        }

        internal static LogicalColumnWriter<TElementType> Create<TElementType>(ColumnWriter columnWriter, int bufferLength = 4 * 1024)
        {
            return (LogicalColumnWriter<TElementType>) Create(columnWriter, bufferLength);
        }

        public abstract TReturn Apply<TReturn>(ILogicalColumnWriterVisitor<TReturn> visitor);
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

    internal sealed class LogicalColumnWriter<TPhysicalValue, TLogicalValue, TElement> : LogicalColumnWriter<TElement>
        where TPhysicalValue : unmanaged
    {
        internal LogicalColumnWriter(ColumnWriter columnWriter, int bufferLength)
            : base(columnWriter, bufferLength)
        {
            _byteBuffer = typeof(TPhysicalValue) == typeof(ByteArray) ? new ByteBuffer(bufferLength) : null;
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
        }

        public override void WriteBatch(TElement[] values, int start, int length)
        {
            // Convert logical values into physical values at the lowest array level
            var converter = LogicalWrite<TLogicalValue, TPhysicalValue>.GetConverter(_byteBuffer);

            // Handle arrays separately
            if (typeof(TElement) != typeof(byte[]) && typeof(TElement).IsArray)
            {
                WriteBatchArray(values, start, length, converter);
                return;
            }

            WriteBatchSimple(((TLogicalValue[]) (object) values).AsSpan(start, length), converter);
        }

        private void WriteBatchArray(TElement[] values, int start, int length, LogicalWrite<TLogicalValue, TPhysicalValue>.Converter converter)
        {
            WriteArrayInternal(values, start, length, 0, NestingDepth, 0, NullDefinitionLevels, ColumnDescriptor.MaxDefinitionlevel, converter);
        }

        private void WriteArrayInternal(
            Array values, int start, int length,
            short repetitionLevel, short maxRepetitionLevel, short leafFirstRepLevel, 
            short[] nullDefinitionLevels, short leafDefinitionLevel,
            LogicalWrite<TLogicalValue, TPhysicalValue>.Converter converter)
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
                var columnWriter = (ColumnWriter<TPhysicalValue>) Source;
                var buffer = (TPhysicalValue[]) Buffer;

                while (rowsWritten < length)
                {
                    var bufferLength = Math.Min(length - rowsWritten, buffer.Length);

                    converter(((TLogicalValue[]) values).AsSpan(start + rowsWritten), DefLevels, buffer, nullDefinitionLevel);

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

                    var columnWriter = (ColumnWriter<TPhysicalValue>) Source;

                    columnWriter.WriteBatchSpaced(
                        1, new[] {nullDefinitionLevel}, new[] {innerLeafFirstRepLevel}, 
                        new byte[] {0}, 0, new TPhysicalValue[] { });
                }

                writtenFirstItem = true;
            }
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private void WriteBatchSimple(ReadOnlySpan<TLogicalValue> values, LogicalWrite<TLogicalValue, TPhysicalValue>.Converter converter)
        {
            var rowsWritten = 0;
            var nullLevel = DefLevels == null ? (short) -1 : (short) 0;
            var columnWriter = (ColumnWriter<TPhysicalValue>) Source;
            var buffer = (TPhysicalValue[]) Buffer;

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
