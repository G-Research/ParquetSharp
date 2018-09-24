using System;
using System.Collections.Generic;

namespace ParquetSharp
{
    /// <summary>
    /// Column reader transparently converting Parquet physical types to C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnReader : LogicalColumnStream<ColumnReader>
    {
        protected LogicalColumnReader(ColumnReader columnReader, Type elementType, int bufferLength)
            : base(columnReader, columnReader.ColumnDescriptor, elementType, columnReader.ElementType, bufferLength)
        {
        }

        internal static LogicalColumnReader Create(ColumnReader columnReader, int bufferLength)
        {
            if (columnReader == null) throw new ArgumentNullException(nameof(columnReader));

            return columnReader.ColumnDescriptor.Apply(new Creator(columnReader, bufferLength));
        }

        internal static LogicalColumnReader<TElement> Create<TElement>(ColumnReader columnReader, int bufferLength)
        {
            var reader = Create(columnReader, bufferLength);

            try
            {
                return (LogicalColumnReader<TElement>) reader;
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }

        public bool HasNext => Source.HasNext;

        public abstract TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor);

        private sealed class Creator : IColumnDescriptorVisitor<LogicalColumnReader>
        {
            public Creator(ColumnReader columnReader, int bufferLength)
            {
                _columnReader = columnReader;
                _bufferLength = bufferLength;
            }

            public LogicalColumnReader OnColumnDescriptor<TPhysical, TLogical, TElement>() where TPhysical : unmanaged
            {
                return new LogicalColumnReader<TPhysical, TLogical, TElement>(_columnReader, _bufferLength);
            }

            private readonly ColumnReader _columnReader;
            private readonly int _bufferLength;
        }
    }

    public abstract class LogicalColumnReader<TElement> : LogicalColumnReader
    {
        protected LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, typeof(TElement), bufferLength)
        {
        }

        public override TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnReader(this);
        }

        public TElement[] ReadAll(int rows)
        {
            var values = new TElement[rows];
            var read = ReadBatch(values, 0, values.Length);

            if (read != rows)
            {
                throw new ArgumentException($"read {read} rows, expected {rows} rows");
            }

            return values;
        }

        public abstract int ReadBatch(TElement[] destination, int start, int length);
    }

    internal sealed class LogicalColumnReader<TPhysical, TLogical, TElement> : LogicalColumnReader<TElement>
        where TPhysical : unmanaged
    {
        internal LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, bufferLength)
        {
            _bufferedReader = new BufferedReader<TPhysical>(Source, (TPhysical[]) Buffer, DefLevels, RepLevels);
        }

        public override int ReadBatch(TElement[] destination, int start, int length)
        {
            var converter = LogicalRead<TLogical, TPhysical>.GetConverter(LogicalType, ColumnDescriptor.TypeScale);

            // Handle arrays separately as they are nested structures.
            if (typeof(TElement) != typeof(byte[]) && typeof(TElement).IsArray)
            {
                return ReadBatchArray(destination, start, length, converter);
            }

            // Otherwise deal with flat values.
            return ReadBatchSimple(((TLogical[]) (object) destination).AsSpan(start, length), converter);
        }

        private int ReadBatchArray(TElement[] destination, int start, int length, LogicalRead<TLogical, TPhysical>.Converter converter)
        {
            var result = ReadArrayInternal(0, NestingDepth, NullDefinitionLevels, length, _bufferedReader, converter, typeof(TElement));
            result.CopyTo(destination, start);
            return result.Length;
        }

        private static Array ReadArrayInternal(
            short repetitionLevel, short maxRepetitionLevel, short[] nullDefinitionLevels, int numArrayEntriesToRead, 
            BufferedReader<TPhysical> valueReader, LogicalRead<TLogical, TPhysical>.Converter converter, Type elementType)
        {
            var nullDefinitionLevel = nullDefinitionLevels[repetitionLevel];

            // Check if we are at the leaf schema node.
            if (maxRepetitionLevel == repetitionLevel)
            {
                var defnLevel = new List<short>();
                var values = new List<TPhysical>();
                var firstValue = true;

                while (!valueReader.IsEofDefinition)
                {
                    var defn = valueReader.GetCurrentDefinition();

                    if (!firstValue && defn.RepLevel < repetitionLevel)
                    {
                        break;
                    }

                    if (defn.DefLevel < nullDefinitionLevel)
                    {
                        throw new Exception("Invalid input stream.");
                    }

                    if (defn.DefLevel > nullDefinitionLevel)
                    {
                        values.Add(valueReader.ReadValue());
                    }

                    defnLevel.Add(defn.DefLevel);

                    valueReader.NextDefinition();
                    firstValue = false;
                }

                var dest = new TLogical[defnLevel.Count];
                converter(values.ToArray(), defnLevel.ToArray(), dest, nullDefinitionLevel);
                return dest;
            }
            
            // Else read the underlying levels.
            {
                var acc = new List<Array>();

                while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
                {
                    var defn = valueReader.GetCurrentDefinition();

                    if (defn.RepLevel > repetitionLevel)
                    {
                        throw new Exception("Invalid Parquet input - only homogenous jagged arrays supported.");
                    }

                    if (defn.DefLevel > nullDefinitionLevel)
                    {
                        acc.Add(ReadArrayInternal(
                            (short) (repetitionLevel + 1), maxRepetitionLevel, nullDefinitionLevels, -1,
                            valueReader, converter, elementType.GetElementType()));
                    }
                    else
                    {
                        acc.Add(null);
                        valueReader.NextDefinition();
                    }

                    if (valueReader.IsEofDefinition)
                    {
                        break;
                    }

                    defn = valueReader.GetCurrentDefinition();

                    if (defn.RepLevel < repetitionLevel)
                    {
                        break;
                    }
                }

                return ListToArray(acc, elementType);
            }
        }

        private static Array ListToArray(List<Array> list, Type elementType)
        {
            var result = Array.CreateInstance(elementType, list.Count);

            for (int i = 0; i != list.Count; ++i)
            {
                result.SetValue(list[i], i);
            }

            return result;
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private int ReadBatchSimple(Span<TLogical> destination, LogicalRead<TLogical, TPhysical>.Converter converter)
        {
            var rowsRead = 0;
            var nullLevel = DefLevels == null ? (short)-1 : (short)0;
            var columnReader = (ColumnReader<TPhysical>)Source;

            var buffer = (TPhysical[]) Buffer;

            while (rowsRead < destination.Length && HasNext)
            {
                var toRead = Math.Min(destination.Length - rowsRead, Buffer.Length);
                var read = checked((int) columnReader.ReadBatch(toRead, DefLevels, RepLevels, buffer, out var valuesRead));
                converter(buffer.AsSpan(0, checked((int) valuesRead)), DefLevels, destination.Slice(rowsRead, read), nullLevel);
                rowsRead += read;
            }

            return rowsRead;
        }

        private readonly BufferedReader<TPhysical> _bufferedReader;
    }
}
