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
        }

        public override void Dispose()
        {
            _byteBuffer?.Dispose();

            base.Dispose();
        }

        public override void WriteBatch(ReadOnlySpan<TElement> values)
        {
            // Convert logical values into physical values at the lowest array level
            var converter = LogicalWrite<TLogical, TPhysical>.GetConverter(LogicalType, ColumnDescriptor.TypeScale, _byteBuffer);

            // Handle arrays separately
            if (typeof(TElement) != typeof(byte[]) && typeof(TElement).IsArray)
            {
                if (NestingDepth == 0)
                {
                    WriteArrayFinalLevel(values, 0, 0, ColumnDescriptor.MaxDefinitionLevel, converter, NullDefinitionLevels[0]);
                }
                else
                {
                    WriteArrayIntermediateLevel(values, 0, NestingDepth, 0, NullDefinitionLevels, ColumnDescriptor.MaxDefinitionLevel, converter);
                }
            }
            else
            {
                WriteBatchSimple(values, converter as LogicalWrite<TElement, TPhysical>.Converter);
            }
        }

        private void WriteArrayIntermediateLevel(
            ReadOnlySpan<TElement> values,
            short repetitionLevel, short maxRepetitionLevel, short leafFirstRepLevel, 
            short[] nullDefinitionLevels, short leafDefinitionLevel, 
            LogicalWrite<TLogical, TPhysical>.Converter converter)
        {
            var firstItem = true;

            for (int i = 0; i != values.Length; ++i)
            {
                WriteArrayNextLevel(
                    values[i] as Array,
                    repetitionLevel, maxRepetitionLevel, leafFirstRepLevel,
                    nullDefinitionLevels, leafDefinitionLevel,
                    converter,
                    ref firstItem);
            }
        }

        private void WriteArrayIntermediateLevel(
            Array values,
            short repetitionLevel, short maxRepetitionLevel, short leafFirstRepLevel,
            short[] nullDefinitionLevels, short leafDefinitionLevel,
            LogicalWrite<TLogical, TPhysical>.Converter converter)
        {
            var firstItem = true;

            for (int i = 0; i != values.Length; ++i)
            {
                WriteArrayNextLevel(
                    (Array) values.GetValue(i), 
                    repetitionLevel, maxRepetitionLevel, leafFirstRepLevel, 
                    nullDefinitionLevels, leafDefinitionLevel,
                    converter, 
                    ref firstItem);
            }
        }

        private void WriteArrayNextLevel(
            Array item,
            short repetitionLevel, short maxRepetitionLevel, short leafFirstRepLevel,
            short[] nullDefinitionLevels, short leafDefinitionLevel,
            LogicalWrite<TLogical, TPhysical>.Converter converter,
            ref bool firstItem)
        {
            var nullDefinitionLevel = nullDefinitionLevels[repetitionLevel];
            var innerLeafFirstRepLevel = firstItem ? leafFirstRepLevel : repetitionLevel;

            if (item != null)
            {
                var nextLevel = (short) (repetitionLevel + 1);
                if (nextLevel == maxRepetitionLevel)
                {
                    WriteArrayFinalLevel<TLogical>(
                        (TLogical[]) item,
                        nextLevel, innerLeafFirstRepLevel, leafDefinitionLevel,
                        converter,
                        nullDefinitionLevels[nextLevel]);
                }
                else
                {
                    WriteArrayIntermediateLevel(
                        item,
                        nextLevel, maxRepetitionLevel, innerLeafFirstRepLevel,
                        nullDefinitionLevels, leafDefinitionLevel,
                        converter);
                }
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

            firstItem = false;
        }

        /// <summary>
        /// Write implementation for writing the deepest level array.
        /// </summary>
        private void WriteArrayFinalLevel<TTLogical>(
            ReadOnlySpan<TTLogical> values, 
            short repetitionLevel, short leafFirstRepLevel, 
            short leafDefinitionLevel, 
            LogicalWrite<TLogical, TPhysical>.Converter converter, 
            short nullDefinitionLevel)
        {
            if (typeof(TTLogical) != typeof(TLogical)) throw new ArgumentException("generic logical type should never be different");
            if (converter == null) throw new ArgumentNullException(nameof(converter));
            if (nullDefinitionLevel != -1 && DefLevels == null) throw new ArgumentException("internal error: DefLevels should not be null.");

            var rowsWritten = 0;
            var columnWriter = (ColumnWriter<TPhysical>) Source;
            var buffer = (TPhysical[]) Buffer;
            var convert = converter as LogicalWrite<TTLogical, TPhysical>.Converter;
            var firstItem = true;

            if (convert == null)
            {
                throw new InvalidCastException("generic logical type should never be different");
            }

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, buffer.Length);

                convert(values.Slice(rowsWritten, bufferLength), DefLevels, buffer, nullDefinitionLevel);

                for (int i = 0; i < bufferLength; i++)
                {
                    RepLevels[i] = repetitionLevel;

                    // If the leaves are required, we have to write the deflevel because the converter won't do this for us.
                    if (nullDefinitionLevel == -1)
                    {
                        DefLevels[i] = leafDefinitionLevel;
                    }
                }

                if (firstItem)
                {
                    RepLevels[0] = leafFirstRepLevel;
                }

                columnWriter.WriteBatch(bufferLength, DefLevels, RepLevels, buffer);
                rowsWritten += bufferLength;
                firstItem = false;

                _byteBuffer?.Clear();
            }
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private void WriteBatchSimple<TTLogical>(ReadOnlySpan<TTLogical> values, LogicalWrite<TTLogical, TPhysical>.Converter converter)
        {
            if (typeof(TTLogical) != typeof(TLogical)) throw new ArgumentException("generic logical type should never be different");
            if (converter == null) throw new ArgumentNullException(nameof(converter));

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
