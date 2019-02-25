﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ParquetSharp.Schema;

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

    public abstract class LogicalColumnReader<TElement> : LogicalColumnReader, IEnumerable<TElement>
    {
        protected LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, typeof(TElement), bufferLength)
        {
        }

        public override TReturn Apply<TReturn>(ILogicalColumnReaderVisitor<TReturn> visitor)
        {
            return visitor.OnLogicalColumnReader(this);
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            var buffer = new TElement[BufferLength];

            while (HasNext)
            {
                var read = ReadBatch(buffer);

                for (int i = 0; i != read; ++i)
                {
                    yield return buffer[i];
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public TElement[] ReadAll(int rows)
        {
            var values = new TElement[rows];
            var read = ReadBatch(values);

            if (read != rows)
            {
                throw new ArgumentException($"read {read} rows, expected {rows} rows");
            }

            return values;
        }

        public int ReadBatch(TElement[] destination, int start, int length)
        {
            return ReadBatch(destination.AsSpan(start, length));
        }

        public abstract int ReadBatch(Span<TElement> destination);
    }

    internal sealed class LogicalColumnReader<TPhysical, TLogical, TElement> : LogicalColumnReader<TElement>
        where TPhysical : unmanaged
    {
        internal LogicalColumnReader(ColumnReader columnReader, int bufferLength)
            : base(columnReader, bufferLength)
        {
            _bufferedReader = new BufferedReader<TPhysical>(Source, (TPhysical[]) Buffer, DefLevels, RepLevels);
        }

        public override int ReadBatch(Span<TElement> destination)
        {
            var converter = LogicalRead<TLogical, TPhysical>.GetConverter(LogicalType, ColumnDescriptor.TypeScale);

            // Handle arrays separately as they are nested structures.
            if (typeof(TElement) != typeof(byte[]) && typeof(TElement).IsArray)
            {
                return ReadBatchArray(destination, converter);
            }

            // Otherwise deal with flat values.
            return ReadBatchSimple(destination, converter as LogicalRead<TElement, TPhysical>.Converter);
        }

        private static List<Node> GetSchemaNode(Node node)
        {
            var schemaNodes = new List<Node>();
            for (; node != null; node = node.Parent)
            {
                schemaNodes.Add(node);
            }
            schemaNodes.RemoveAt(schemaNodes.Count - 1); // we don't need the schema root
            schemaNodes.Reverse(); // root to leaf
            return schemaNodes;
        }

        private int ReadBatchArray(Span<TElement> destination, LogicalRead<TLogical, TPhysical>.Converter converter)
        {
            var schemaNodes = GetSchemaNode(ColumnDescriptor.SchemaNode).ToArray();

            var arrayReaderFuncs = GetArrayReaderFuncs(schemaNodes, typeof(TElement), converter, 0, 0);

            var result = (Span<TElement>)ReadArrayBetter(arrayReaderFuncs, 0, _bufferedReader, converter, destination.Length);

            result.CopyTo(destination);
            return result.Length;
        }

        private static Array ReadArrayBetter(List<Func<BufferedReader<TPhysical>, Func<Array>, int, Array>> arrayReaderFuncs, int level, BufferedReader<TPhysical> valueReader, LogicalRead<TLogical, TPhysical>.Converter converter, int numArrayEntriesToRead)
        {
            return arrayReaderFuncs[level](valueReader, () => ReadArrayBetter(arrayReaderFuncs, level+1, valueReader, converter, -1), numArrayEntriesToRead);
        }

        private static List<Func<BufferedReader<TPhysical>, Func<Array>, int, Array>> GetArrayReaderFuncs(ReadOnlySpan<Node> schemaNodes, Type elementType, LogicalRead<TLogical, TPhysical>.Converter converter, int repetitionLevel, int nullDefinitionLevel)
        {
            var ret = new List<Func<BufferedReader<TPhysical>, Func<Array>, int, Array>>();

            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2 && (schemaNodes[0] is GroupNode g1) && g1.LogicalType == LogicalType.List
                    && g1.Repetition == Repetition.Optional && (schemaNodes[1] is GroupNode g2)
                    && g2.LogicalType == LogicalType.None && g2.Repetition == Repetition.Repeated)
                {
                    ret.Add((valueReader, readNestedLevel, numArrayEntriesToRead) =>
                    {
                        var acc = new List<Array>();

                        while (numArrayEntriesToRead == -1 || acc.Count < numArrayEntriesToRead)
                        {
                            var defn = valueReader.GetCurrentDefinition();

                            Array newItem = null;

                            if (defn.DefLevel >= nullDefinitionLevel + 2)
                            {
                                newItem = readNestedLevel();
                            }
                            else
                            {
                                if (defn.DefLevel == nullDefinitionLevel + 1)
                                {
                                    newItem = CreateEmptyArray(elementType);
                                }
                                valueReader.NextDefinition();
                            }

                            acc.Add(newItem);

                            if (valueReader.IsEofDefinition || valueReader.GetCurrentDefinition().RepLevel < repetitionLevel)
                            {
                                break;
                            }
                        }

                        return ListToArray(acc, elementType);
                    });

                    var containedSchemaNodes = schemaNodes.Slice(2);
                    var containedType = elementType.GetElementType();

                    ret.AddRange(GetArrayReaderFuncs(containedSchemaNodes, containedType, converter, repetitionLevel + 1, nullDefinitionLevel + 2));

                    return ret;
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                bool optional = schemaNodes[0].Repetition == Repetition.Optional;

                ret.Add((valueReader, readNestedLevel, numArrayEntriesToRead) =>
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

                        if (defn.DefLevel > nullDefinitionLevel || !optional)
                        {
                            values.Add(valueReader.ReadValue());
                        }

                        defnLevel.Add(defn.DefLevel);

                        valueReader.NextDefinition();
                        firstValue = false;
                    }

                    var dest = new TLogical[defnLevel.Count];
                    converter(values.ToArray(), defnLevel.ToArray(), dest, (short)nullDefinitionLevel);
                    return dest;
                });

                return ret;
            }

            throw new Exception("ParquetSharp does not understand the schema used");
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

        private static Array CreateEmptyArray(Type elementType)
        {
            if (elementType.IsArray)
            {
                return Array.CreateInstance(elementType.GetElementType(), 0);
            }

            throw new NotImplementedException();
        }

        /// <summary>
        /// Fast implementation when a column contains only flat primitive values.
        /// </summary>
        private int ReadBatchSimple<TTLogical>(Span<TTLogical> destination, LogicalRead<TTLogical, TPhysical>.Converter converter)
        {
            if (typeof(TTLogical) != typeof(TLogical)) throw new ArgumentException("generic logical type should never be different");
            if (converter == null) throw new ArgumentNullException(nameof(converter));

            var rowsRead = 0;
            var nullLevel = DefLevels == null ? (short) -1 : (short) 0;
            var columnReader = (ColumnReader<TPhysical>) Source;

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
