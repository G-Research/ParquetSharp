using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Writer of physical Parquet values to a single column.
    /// </summary>
    public abstract class ColumnWriter : IDisposable
    {
        internal static ColumnWriter Create(IntPtr handle)
        {
            var type = ExceptionInfo.Return<PhysicalType>(handle, ColumnWriter_Type);

            switch (type)
            {
                case PhysicalType.Boolean:
                    return new ColumnWriter<bool>(handle);
                case PhysicalType.Int32:
                    return new ColumnWriter<int>(handle);
                case PhysicalType.Int64:
                    return new ColumnWriter<long>(handle);
                case PhysicalType.Int96:
                    return new ColumnWriter<Int96>(handle);
                case PhysicalType.Float:
                    return new ColumnWriter<float>(handle);
                case PhysicalType.Double:
                    return new ColumnWriter<double>(handle);
                case PhysicalType.ByteArray:
                    return new ColumnWriter<ByteArray>(handle);
                case PhysicalType.FixedLenByteArray:
                    return new ColumnWriter<FixedLenByteArray>(handle);
                default:
                    throw new NotSupportedException($"Physical type {type} is not supported");
            }
        }

        internal ColumnWriter(IntPtr handle)
        {
            Handle = handle;
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

        public ColumnDescriptor ColumnDescriptor => new ColumnDescriptor(ExceptionInfo.Return<IntPtr>(Handle, ColumnWriter_Descr));
        public long RowWritten => ExceptionInfo.Return<long>(Handle, ColumnWriter_Rows_Written);
        public PhysicalType Type => ExceptionInfo.Return<PhysicalType>(Handle, ColumnWriter_Type);
        public WriterProperties WriterProperties => new WriterProperties(ExceptionInfo.Return<IntPtr>(Handle, ColumnWriter_Properties));

        public abstract Type ElementType { get; }
        public abstract TReturn Apply<TReturn>(IColumnWriterVisitor<TReturn> visitor);

        public LogicalColumnWriter LogicalWriter(int bufferLength = 4 * 1024)
        {
            return LogicalColumnWriter.Create(this, bufferLength);
        }

        public LogicalColumnWriter<TElement> LogicalWriter<TElement>(int bufferLength = 4 * 1024)
        {
            return LogicalColumnWriter.Create<TElement>(this, bufferLength);
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

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Bool(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, bool* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Int32(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, int* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Int64(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, long* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Int96(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, Int96* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Float(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, float* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_Double(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, double* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_ByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, ByteArray* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatch_FixedLenByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, FixedLenByteArray* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Bool(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, bool* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Int32(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, int* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Int64(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, long* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Int96(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, Int96* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Float(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, float* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_Double(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, double* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_ByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, ByteArray* values);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnWriter_WriteBatchSpaced_FixedLenByteArray(
            IntPtr columnWriter, long numValues, short* defLevels, short* repLevels, byte* validBits, long validBitsOffset, FixedLenByteArray* values);

        protected readonly IntPtr Handle;
    }

    /// <inheritdoc />
    public sealed class ColumnWriter<TValue> : ColumnWriter where TValue : unmanaged
    {
        internal ColumnWriter(IntPtr handle)
            : base(handle)
        {
        }

        public override Type ElementType => typeof(TValue);

        public override TReturn Apply<TReturn>(IColumnWriterVisitor<TReturn> visitor)
        {
            return visitor.OnColumnWriter(this);
        }

        public void WriteBatch(ReadOnlySpan<TValue> values)
        {
            WriteBatch(values.Length, null, null, values);
        }

        [Obsolete("Use the WriteBatch(ReadOnlySpan<TValue> values) overload. The numValues argument is redundant.")]
        public void WriteBatch(int numValues, ReadOnlySpan<TValue> values)
        {
            if (numValues > values.Length) throw new ArgumentOutOfRangeException(nameof(numValues), "numValues is larger than length of values");

            WriteBatch(numValues, null, null, values);
        }

        public unsafe void WriteBatch(long numValues, ReadOnlySpan<short> defLevels, ReadOnlySpan<short> repLevels, ReadOnlySpan<TValue> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (defLevels != null && defLevels.Length < numValues) throw new ArgumentOutOfRangeException(nameof(defLevels), "numValues is larger than length of defLevels");
            if (repLevels != null && repLevels.Length < numValues) throw new ArgumentOutOfRangeException(nameof(repLevels), "numValues is larger than length of repLevels");

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
            long numValues, ReadOnlySpan<short> defLevels, ReadOnlySpan<short> repLevels, 
            ReadOnlySpan<byte> validBits, long validBitsOffset, ReadOnlySpan<TValue> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (defLevels == null) throw new ArgumentNullException(nameof(defLevels));
            if (repLevels == null) throw new ArgumentNullException(nameof(repLevels));
            if (validBits == null) throw new ArgumentNullException(nameof(validBits));
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
