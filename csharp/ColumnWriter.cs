using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Writer of physical Parquet values to a single column.
    /// </summary>
    public abstract class ColumnWriter : IDisposable
    {
        internal static ColumnWriter Create(IntPtr handle, RowGroupWriter rowGroupWriter, int columnIndex)
        {
            var type = ExceptionInfo.Return<PhysicalType>(handle, ColumnWriter_Type);

            switch (type)
            {
                case PhysicalType.Boolean:
                    return new ColumnWriter<bool>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.Int32:
                    return new ColumnWriter<int>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.Int64:
                    return new ColumnWriter<long>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.Int96:
                    return new ColumnWriter<Int96>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.Float:
                    return new ColumnWriter<float>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.Double:
                    return new ColumnWriter<double>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.ByteArray:
                    return new ColumnWriter<ByteArray>(handle, rowGroupWriter, columnIndex);
                case PhysicalType.FixedLenByteArray:
                    return new ColumnWriter<FixedLenByteArray>(handle, rowGroupWriter, columnIndex);
                default:
                    throw new NotSupportedException($"Physical type {type} is not supported");
            }
        }

        internal ColumnWriter(IntPtr handle, RowGroupWriter rowGroupWriter, int columnIndex)
        {
            _handle = handle;
            RowGroupWriter = rowGroupWriter;
            ColumnIndex = columnIndex;
        }

        public void Dispose()
        {
            // Do not close in dispose, leave that to ParquetFileWriter AppendRowGroup() and destructor.
            // See https://github.com/G-Research/ParquetSharp/issues/104.
        }

        public long Close()
        {
            return ExceptionInfo.Return<long>(Handle, ColumnWriter_Close);
        }

        /// <summary>
        /// Get the index of the column within the row group.
        /// </summary>
        public int ColumnIndex { get; }
        /// <summary>
        /// Get the <see cref="ParquetSharp.LogicalTypeFactory"/> for the Parquet file writer.
        /// </summary>
        public LogicalTypeFactory LogicalTypeFactory => RowGroupWriter.ParquetFileWriter.LogicalTypeFactory;
        /// <summary>
        /// Get the <see cref="ParquetSharp.LogicalWriteConverterFactory"/> for the Parquet file writer.
        /// </summary>
        public LogicalWriteConverterFactory LogicalWriteConverterFactory => RowGroupWriter.ParquetFileWriter.LogicalWriteConverterFactory;

        /// <summary>
        /// Get the <see cref="ParquetSharp.ColumnDescriptor"/> for the column.
        /// </summary>
        public ColumnDescriptor ColumnDescriptor => new(ExceptionInfo.Return<IntPtr>(Handle, ColumnWriter_Descr));
        /// <summary>
        /// Get the number of rows written to the column so far.
        /// </summary>
        public long RowWritten => ExceptionInfo.Return<long>(Handle, ColumnWriter_Rows_Written);
        /// <summary>
        /// Get the physical type of the column.
        /// </summary>
        public PhysicalType Type => ExceptionInfo.Return<PhysicalType>(Handle, ColumnWriter_Type);
        /// <summary>
        /// Get the <see cref="ParquetSharp.WriterProperties"/> for the column.
        /// </summary>
        public WriterProperties WriterProperties => new(ExceptionInfo.Return<IntPtr>(Handle, ColumnWriter_Properties));

        /// <summary>
        /// Get the element <see cref="Type"/> of the data being written.
        /// </summary>
        public abstract Type ElementType { get; }

        /// <summary>
        /// Apply a visitor to the column writer.
        /// </summary>
        /// <typeparam name="TReturn">The return type of the visitor.</typeparam>
        /// <param name="visitor">The visitor instance.</param>
        /// <returns>The result of the visitor operation.</returns>
        public abstract TReturn Apply<TReturn>(IColumnWriterVisitor<TReturn> visitor);

        /// <summary>
        /// Create a <see cref="LogicalColumnWriter"/>.
        /// </summary>
        /// <param name="bufferLength">The buffer length in bytes. Default is 4KB.</param>
        /// <returns>A <see cref="LogicalColumnWriter"/> instance.</returns>
        public LogicalColumnWriter LogicalWriter(int bufferLength = 4 * 1024)
        {
            return LogicalColumnWriter.Create(this, bufferLength, elementTypeOverride: null);
        }

        /// <summary>
        /// Create a strongly-typed <see cref="LogicalColumnWriter"/> without an explicit element type override.
        /// </summary>
        /// <typeparam name="TElement">The type of the data to write.</typeparam>
        /// <param name="bufferLength">The buffer length in bytes. Default is 4KB.</param>
        /// <returns>A <see cref="LogicalColumnWriter"/> instance.</returns>
        public LogicalColumnWriter<TElement> LogicalWriter<TElement>(int bufferLength = 4 * 1024)
        {
            return LogicalColumnWriter.Create<TElement>(this, bufferLength, elementTypeOverride: null);
        }

        /// <summary>
        /// Create a strongly-typed <see cref="LogicalColumnWriter"/> with an explicit element type override.
        /// </summary>
        /// <typeparam name="TElement">The type of the data to write.</typeparam>
        /// <param name="bufferLength">The buffer length in bytes. Default is 4KB.</param>
        /// <returns>A <see cref="LogicalColumnWriter"/> instance.</returns>
        public LogicalColumnWriter<TElement> LogicalWriterOverride<TElement>(int bufferLength = 4 * 1024)
        {
            return LogicalColumnWriter.Create<TElement>(this, bufferLength, typeof(TElement));
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnWriter_Close(IntPtr columnWriter, out long columnSize);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnWriter_Descr(IntPtr columnWriter, out IntPtr columnDescriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnWriter_Properties(IntPtr columnWriter, out IntPtr writerProperties);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnWriter_Rows_Written(IntPtr columnWriter, out long rowsWritten);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnWriter_Type(IntPtr columnWriter, out PhysicalType type);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Bool(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, bool* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Int32(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, int* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Int64(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, long* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Int96(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, Int96* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Float(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, float* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Double(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, double* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_ByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, ByteArray* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_FixedLenByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, FixedLenByteArray* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Bool(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, bool* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Int32(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, int* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Int64(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, long* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Int96(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, Int96* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Float(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, float* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Double(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, double* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_ByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, ByteArray* values);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_FixedLenByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, FixedLenByteArray* values);

        protected IntPtr Handle
        {
            get
            {
                if (!RowGroupWriter.Buffered)
                {
                    var currentColumn = RowGroupWriter.CurrentColumn;
                    if (ColumnIndex != currentColumn)
                    {
                        throw new Exception($"Writer for column {ColumnIndex} is no longer valid, " +
                                            $"the current column for the row group writer is {currentColumn}");
                    }
                }

                return _handle;
            }
        }

        private readonly IntPtr _handle;
        internal readonly RowGroupWriter RowGroupWriter;
    }

    /// <summary>
    /// Strongly-typed writer of physical Parquet values to a single column.
    /// </summary>
    /// <typeparam name="TValue">The data type of the column.</typeparam>
    public sealed class ColumnWriter<TValue> : ColumnWriter where TValue : unmanaged
    {
        internal ColumnWriter(IntPtr handle, RowGroupWriter rowGroupWriter, int columnIndex)
            : base(handle, rowGroupWriter, columnIndex)
        {
        }

        /// <inheritdoc />
        public override Type ElementType => typeof(TValue);

        /// <inheritdoc />
        public override TReturn Apply<TReturn>(IColumnWriterVisitor<TReturn> visitor)
        {
            return visitor.OnColumnWriter(this);
        }

        /// <summary>
        /// Write a batch of values to the column.
        /// </summary>
        /// <param name="values">The values to write.</param>
        public void WriteBatch(ReadOnlySpan<TValue> values)
        {
            WriteBatch(values.Length, null, null, values);
        }

        /// <summary>
        /// Write a batch of values to the column with optional definition and repetition levels.
        /// </summary>
        /// <param name="numValues">The number of values to write.</param>
        /// <param name="defLevels">The definition levels for the values.</param>
        /// <param name="repLevels">The repetition levels for the values.</param>
        /// <param name="values">The values to write.</param>
        /// <remarks>
        /// The lengths of <paramref name="defLevels"/> and <paramref name="repLevels"/> must be at least <paramref name="numValues"/>.
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="numValues"/> is larger
        /// than the length of <paramref name="defLevels"/> or <paramref name="repLevels"/>.</exception>
        public unsafe void WriteBatch(int numValues, ReadOnlySpan<short> defLevels, ReadOnlySpan<short> repLevels, ReadOnlySpan<TValue> values)
        {
            if (!defLevels.IsEmpty && defLevels.Length < numValues) throw new ArgumentOutOfRangeException(nameof(defLevels), "numValues is larger than length of defLevels");
            if (!repLevels.IsEmpty && repLevels.Length < numValues) throw new ArgumentOutOfRangeException(nameof(repLevels), "numValues is larger than length of repLevels");

            var type = typeof(TValue);

            fixed (short* pDefLevels = defLevels)
            fixed (short* pRepLevels = repLevels)
            fixed (TValue* pValues = values)
            {
                if (type == typeof(bool))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_Bool(Handle,
                        numValues, pDefLevels, pRepLevels, (bool*) pValues));
                    return;
                }

                if (type == typeof(int))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_Int32(Handle,
                        numValues, pDefLevels, pRepLevels, (int*) pValues));
                    return;
                }

                if (type == typeof(long))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_Int64(Handle,
                        numValues, pDefLevels, pRepLevels, (long*) pValues));
                    return;
                }

                if (type == typeof(Int96))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_Int96(Handle,
                        numValues, pDefLevels, pRepLevels, (Int96*) pValues));
                    return;
                }

                if (type == typeof(float))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_Float(Handle,
                        numValues, pDefLevels, pRepLevels, (float*) pValues));
                    return;
                }

                if (type == typeof(double))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_Double(Handle,
                        numValues, pDefLevels, pRepLevels, (double*) pValues));
                    return;
                }

                if (type == typeof(ByteArray))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_ByteArray(Handle,
                        numValues, pDefLevels, pRepLevels, (ByteArray*) pValues));
                    return;
                }

                if (type == typeof(FixedLenByteArray))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatch_FixedLenByteArray(Handle,
                        numValues, pDefLevels, pRepLevels, (FixedLenByteArray*) pValues));
                    return;
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }

        public unsafe void WriteBatchSpaced(
            int numValues, ReadOnlySpan<short> defLevels, ReadOnlySpan<short> repLevels,
            ReadOnlySpan<byte> validBits, long validBitsOffset, ReadOnlySpan<TValue> values)
        {
            //if (values.Length < numValues) throw new ArgumentOutOfRangeException("numValues is larger than length of values");
            if (defLevels.Length < numValues) throw new ArgumentOutOfRangeException(nameof(defLevels), "numValues is larger than length of defLevels");
            if (repLevels.Length < numValues) throw new ArgumentOutOfRangeException(nameof(repLevels), "numValues is larger than length of repLevels");
            // https://stackoverflow.com/questions/17944/how-to-round-up-the-result-of-integer-division
            if (validBits.Length < (validBitsOffset + numValues + 7) / 8) throw new ArgumentOutOfRangeException(nameof(validBits), "numValues is larger than the bit length of validBits");

            var type = typeof(TValue);

            fixed (short* pDefLevels = defLevels)
            fixed (short* pRepLevels = repLevels)
            fixed (byte* pValidBits = validBits)
            fixed (TValue* pValues = values)
            {
                if (type == typeof(bool))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_Bool(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (bool*) pValues));
                    return;
                }

                if (type == typeof(int))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_Int32(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (int*) pValues));
                    return;
                }

                if (type == typeof(long))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_Int64(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (long*) pValues));
                    return;
                }

                if (type == typeof(Int96))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_Int96(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (Int96*) pValues));
                    return;
                }

                if (type == typeof(float))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_Float(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (float*) pValues));
                    return;
                }

                if (type == typeof(double))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_Double(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (double*) pValues));
                    return;
                }

                if (type == typeof(ByteArray))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_ByteArray(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (ByteArray*) pValues));
                    return;
                }

                if (type == typeof(FixedLenByteArray))
                {
                    ExceptionInfo.Check(TypedColumnWriter_WriteBatchSpaced_FixedLenByteArray(Handle,
                        numValues, pDefLevels, pRepLevels, pValidBits, validBitsOffset, (FixedLenByteArray*) pValues));
                    return;
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }
    }
}
