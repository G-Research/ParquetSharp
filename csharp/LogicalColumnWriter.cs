﻿using System;
using System.Collections.Generic;
using ParquetSharp.Schema;

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
                WriteArray(values.ToArray(), ArraySchemaNodes, typeof(TElement), converter, 0, 0, 0);
            }
            else
            {
                WriteBatchSimple(values, converter as LogicalWrite<TElement, TPhysical>.Converter);
            }
        }

        private void WriteArray(Array array, ReadOnlySpan<Node> schemaNodes, Type elementType, LogicalWrite<TLogical, TPhysical>.Converter converter, short repetitionLevel, short nullDefinitionLevel, short firstLeafRepLevel)
        {
            if (elementType.IsArray && elementType != typeof(byte[]))
            {
                if (schemaNodes.Length >= 2
                    && (schemaNodes[0] is GroupNode g1) && g1.LogicalType == LogicalType.List && g1.Repetition == Repetition.Optional
                    && (schemaNodes[1] is GroupNode g2) && g2.LogicalType == LogicalType.None && g2.Repetition == Repetition.Repeated)
                {
                    var containedType = elementType.GetElementType();

                    WriteArrayIntermediateLevel(
                        array,
                        schemaNodes.Slice(2),
                        containedType,
                        converter,
                        nullDefinitionLevel,
                        repetitionLevel,
                        firstLeafRepLevel
                    );

                    return;
                }

                throw new Exception("elementType is an array but schema does not match the expected layout");
            }

            if (schemaNodes.Length == 1)
            {
                bool isOptional = schemaNodes[0].Repetition == Repetition.Optional;

                short leafDefinitionLevel = isOptional ? (short)(nullDefinitionLevel + 1) : nullDefinitionLevel;
                short leafNullDefinitionLevel = isOptional ? nullDefinitionLevel : (short)-1;

                WriteArrayFinalLevel(array, repetitionLevel, firstLeafRepLevel, leafDefinitionLevel, converter, leafNullDefinitionLevel);

                return;
            }

            throw new Exception("ParquetSharp does not understand the schema used");
        }

        private void WriteArrayIntermediateLevel(Array values, ReadOnlySpan<Node> schemaNodes, Type elementType, LogicalWrite<TLogical, TPhysical>.Converter converter, short nullDefinitionLevel, short repetitionLevel, short firstLeafRepLevel)
        {
            var columnWriter = (ColumnWriter<TPhysical>)Source;

            for (var i = 0; i < values.Length; i++)
            {
                var currentLeafRepLevel = i > 0 ? repetitionLevel : firstLeafRepLevel;

                var item = values.GetValue(i);

                if (item != null)
                {
                    if (!(item is Array a))
                    {
                        throw new Exception("non-array encountered at non-leaf level");
                    }
                    if (a.Length > 0)
                    {
                        // We have a positive length array, call the top level array writer on its values
                        WriteArray(a, schemaNodes, elementType, converter, (short)(repetitionLevel + 1), (short)(nullDefinitionLevel + 2), currentLeafRepLevel);
                    }
                    else
                    {
                        // Write that we have a zero length array
                        columnWriter.WriteBatchSpaced(1, new[] { (short)(nullDefinitionLevel + 1) }, new[] { currentLeafRepLevel }, new byte[] { 0 }, 0, new TPhysical[] { });
                    }
                }
                else
                {
                    // Write that this item is null
                    columnWriter.WriteBatchSpaced(1, new[] { nullDefinitionLevel }, new[] { currentLeafRepLevel }, new byte[] { 0 }, 0, new TPhysical[] { });
                }
            }
        }

        /// <summary>
        /// Write implementation for writing the deepest level array.
        /// </summary>
        private void WriteArrayFinalLevel(
            Array values, 
            short repetitionLevel, short leafFirstRepLevel, 
            short leafDefinitionLevel, 
            LogicalWrite<TLogical, TPhysical>.Converter converter, 
            short nullDefinitionLevel)
        {
            ReadOnlySpan<TLogical> valuesSpan = (TLogical[])values;

            if (converter == null) throw new ArgumentNullException(nameof(converter));
            if (DefLevels == null) throw new ArgumentException("internal error: DefLevels should not be null.");

            var rowsWritten = 0;
            var columnWriter = (ColumnWriter<TPhysical>) Source;
            var buffer = (TPhysical[]) Buffer;
            var firstItem = true;

            while (rowsWritten < values.Length)
            {
                var bufferLength = Math.Min(values.Length - rowsWritten, buffer.Length);

                converter(valuesSpan.Slice(rowsWritten, bufferLength), DefLevels, buffer, nullDefinitionLevel);

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
