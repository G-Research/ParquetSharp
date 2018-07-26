using System;
using System.Collections.Generic;

namespace ParquetSharp
{
    /// <summary>
    /// Column reader transparently converting Parquet physical types to C# types.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public abstract class LogicalColumnReader : LogicalColumnStream<LogicalColumnReader, ColumnReader>
    {
        protected LogicalColumnReader(ColumnReader columnReader, Type elementType, int bufferLength)
            : base(columnReader, columnReader.ColumnDescriptor, elementType, columnReader.ElementType, bufferLength)
        {
        }

        internal static LogicalColumnReader Create(ColumnReader columnReader, int bufferLength)
        {
            if (columnReader == null) throw new ArgumentNullException(nameof(columnReader));

            return Create(typeof(LogicalColumnReader<,,>), columnReader.ColumnDescriptor, columnReader, bufferLength);
        }

        internal static LogicalColumnReader<TElement> Create<TElement>(ColumnReader columnReader, int bufferLength)
        {
            return (LogicalColumnReader<TElement>) Create(columnReader, bufferLength);
        }

        public bool HasNext => Source.HasNext;

        public abstract TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor);
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

    internal sealed class LogicalColumnReader<TPhysicalValue, TLogicalValue, TElement> : LogicalColumnReader<TElement>
        where TPhysicalValue : unmanaged
    {
        internal LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, bufferLength)
        {
            _bufferedReader = new BufferedReader<TPhysicalValue>(Source, (TPhysicalValue[]) Buffer, DefLevels, RepLevels);
        }

        public override int ReadBatch(TElement[] destination, int start, int length)
        {
            var converter = LogicalRead<TLogicalValue, TPhysicalValue>.GetConverter();

            // Handle arrays separately as they are nested structures.
            if (typeof(TElement) != typeof(byte[]) && typeof(TElement).IsArray)
            {
                return ReadBatchArray(destination, start, length, converter);
            }

            // Otherwise deal with flat values.
            return ReadBatchSimple(((TLogicalValue[]) (object) destination).AsSpan(start, length), converter);
        }

        private int ReadBatchArray(TElement[] destination, int start, int length, LogicalRead<TLogicalValue, TPhysicalValue>.Converter converter)
        {
            var result = ReadArrayInternal(0, NestingDepth, NullDefinitionLevels, length, _bufferedReader, converter, typeof(TElement));
            result.CopyTo(destination, start);
            return result.Length;
        }

        private static Array ReadArrayInternal(
            short repetitionLevel, short maxRepetitionLevel, short[] nullDefinitionLevels, int numArrayEntriesToRead, 
            BufferedReader<TPhysicalValue> valueReader, LogicalRead<TLogicalValue, TPhysicalValue>.Converter converter, Type elementType)
        {
            var nullDefinitionLevel = nullDefinitionLevels[repetitionLevel];

            // Check if we are at the leaf schema node.
            if (maxRepetitionLevel == repetitionLevel)
            {
                var defnLevel = new List<short>();
                var values = new List<TPhysicalValue>();
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

                var dest = new TLogicalValue[defnLevel.Count];
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
        private int ReadBatchSimple(Span<TLogicalValue> destination, LogicalRead<TLogicalValue, TPhysicalValue>.Converter converter)
        {
            var rowsRead = 0;
            var nullLevel = DefLevels == null ? (short)-1 : (short)0;
            var columnReader = (ColumnReader<TPhysicalValue>)Source;

            var buffer = (TPhysicalValue[]) Buffer;

            while (rowsRead < destination.Length && HasNext)
            {
                var toRead = Math.Min(destination.Length - rowsRead, Buffer.Length);
                var read = checked((int) columnReader.ReadBatch(toRead, DefLevels, RepLevels, buffer, out var valuesRead));
                converter(buffer.AsSpan(0, checked((int) valuesRead)), DefLevels, destination.Slice(rowsRead, read), nullLevel);
                rowsRead += read;
            }

            return rowsRead;
        }

        private readonly BufferedReader<TPhysicalValue> _bufferedReader;
    }
}
