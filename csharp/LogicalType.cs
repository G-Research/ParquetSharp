using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public enum LogicalTypeEnum
    {
        Undefined = 0,
        String = 1,
        Map = 2,
        List = 3,
        Enum = 4,
        Decimal = 5,
        Date = 6,
        Time = 7,
        Timestamp = 8,
        Interval = 9,
        Int = 10,
        Nil = 11,
        Json = 12,
        Bson = 13,
        Uuid = 14,
        Float16 = 15,
        None = 16
    }

    public abstract class LogicalType : IDisposable, IEquatable<LogicalType>
    {
        protected LogicalType(IntPtr handle)
        {
            Handle = new ParquetHandle(handle, LogicalType_Free);
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public LogicalTypeEnum Type => ExceptionInfo.Return<LogicalTypeEnum>(Handle, LogicalType_Type);

        public bool Equals(LogicalType? other)
        {
            if (other == null) return false;
            if (Handle.IntPtr == IntPtr.Zero || other.Handle.IntPtr == IntPtr.Zero) return false;

            var equals = ExceptionInfo.Return<IntPtr, bool>(Handle, other.Handle.IntPtr, LogicalType_Equals);
            GC.KeepAlive(other.Handle);
            return equals;
        }

        public override string ToString()
        {
            return ExceptionInfo.ReturnString(Handle, LogicalType_ToString, LogicalType_ToString_Free);
        }

        public static LogicalType String() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_String));
        public static LogicalType Map() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_Map));
        public static LogicalType List() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_List));
        public static LogicalType Enum() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_Enum));
        public static LogicalType Decimal(int precision, int scale = 0) => Create(ExceptionInfo.Return<int, int, IntPtr>(precision, scale, LogicalType_Decimal));
        public static LogicalType Date() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_Date));
        public static LogicalType Time(bool isAdjustedToUtc, TimeUnit timeUnit) => Create(ExceptionInfo.Return<bool, TimeUnit, IntPtr>(isAdjustedToUtc, timeUnit, LogicalType_Time));
        public static LogicalType Timestamp(bool isAdjustedToUtc, TimeUnit timeUnit) => Create(ExceptionInfo.Return<bool, TimeUnit, bool, IntPtr>(isAdjustedToUtc, timeUnit, false, LogicalType_Timestamp));
        public static LogicalType Timestamp(bool isAdjustedToUtc, TimeUnit timeUnit, bool forceSetConvertedType) => Create(ExceptionInfo.Return<bool, TimeUnit, bool, IntPtr>(isAdjustedToUtc, timeUnit, forceSetConvertedType, LogicalType_Timestamp));
        public static LogicalType Interval() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_Interval));
        public static LogicalType Int(int bitWidth, bool isSigned) => Create(ExceptionInfo.Return<int, bool, IntPtr>(bitWidth, isSigned, LogicalType_Int));
        public static LogicalType Null() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_Null));
        public static LogicalType Json() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_JSON));
        public static LogicalType Bson() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_BSON));
        public static LogicalType Uuid() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_UUID));
        public static LogicalType None() => Create(ExceptionInfo.Return<IntPtr>(LogicalType_None));

        internal static LogicalType Create(IntPtr handle)
        {
            if (handle == IntPtr.Zero)
            {
                throw new ArgumentNullException(nameof(handle));
            }

            var type = ExceptionInfo.Return<LogicalTypeEnum>(handle, LogicalType_Type);

            return type switch
            {
                LogicalTypeEnum.String => new StringLogicalType(handle),
                LogicalTypeEnum.Map => new MapLogicalType(handle),
                LogicalTypeEnum.List => new ListLogicalType(handle),
                LogicalTypeEnum.Enum => new EnumLogicalType(handle),
                LogicalTypeEnum.Decimal => new DecimalLogicalType(handle),
                LogicalTypeEnum.Date => new DateLogicalType(handle),
                LogicalTypeEnum.Time => new TimeLogicalType(handle),
                LogicalTypeEnum.Timestamp => new TimestampLogicalType(handle),
                LogicalTypeEnum.Interval => new IntervalLogicalType(handle),
                LogicalTypeEnum.Int => new IntLogicalType(handle),
                LogicalTypeEnum.Nil => new NullLogicalType(handle),
                LogicalTypeEnum.Json => new JsonLogicalType(handle),
                LogicalTypeEnum.Bson => new BsonLogicalType(handle),
                LogicalTypeEnum.Uuid => new UuidLogicalType(handle),
                LogicalTypeEnum.None => new NoneLogicalType(handle),
                _ => throw new ArgumentOutOfRangeException($"unknown logical type {type}")
            };
        }

        [DllImport(ParquetDll.Name)]
        private static extern void LogicalType_Free(IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Type(IntPtr logicalType, out LogicalTypeEnum type);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Equals(IntPtr left, IntPtr right, [MarshalAs(UnmanagedType.I1)] out bool equals);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_ToString(IntPtr logicalType, out IntPtr toString);

        [DllImport(ParquetDll.Name)]
        private static extern void LogicalType_ToString_Free(IntPtr toString);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_String(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Map(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_List(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Enum(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Decimal(int precision, int scale, out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Date(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Time([MarshalAs(UnmanagedType.I1)] bool isAdjustedToUtc, TimeUnit timeUnit, out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Timestamp([MarshalAs(UnmanagedType.I1)] bool isAdjustedToUtc, TimeUnit timeUnit, bool forceSetConvertedType, out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Interval(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Int(int bitWidth, [MarshalAs(UnmanagedType.I1)] bool isSigned, out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_Null(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_JSON(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_BSON(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_UUID(out IntPtr logicalType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr LogicalType_None(out IntPtr logicalType);

        internal readonly ParquetHandle Handle;
    }

    public sealed class StringLogicalType : LogicalType
    {
        internal StringLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class MapLogicalType : LogicalType
    {
        internal MapLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class ListLogicalType : LogicalType
    {
        internal ListLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class EnumLogicalType : LogicalType
    {
        internal EnumLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class DecimalLogicalType : LogicalType
    {
        internal DecimalLogicalType(IntPtr handle) : base(handle) { }

        public int Precision => ExceptionInfo.Return<int>(Handle, DecimalLogicalType_Precision);
        public int Scale => ExceptionInfo.Return<int>(Handle, DecimalLogicalType_Scale);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr DecimalLogicalType_Precision(IntPtr logicalType, out int precision);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr DecimalLogicalType_Scale(IntPtr logicalType, out int scale);
    }

    public sealed class DateLogicalType : LogicalType
    {
        internal DateLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class TimeLogicalType : LogicalType
    {
        internal TimeLogicalType(IntPtr handle) : base(handle) { }

        public bool IsAdjustedToUtc => ExceptionInfo.Return<bool>(Handle, TimeLogicalType_IsAdjustedToUtc);
        public TimeUnit TimeUnit => ExceptionInfo.Return<TimeUnit>(Handle, TimeLogicalType_TimeUnit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr TimeLogicalType_IsAdjustedToUtc(IntPtr logicalType, [MarshalAs(UnmanagedType.I1)] out bool isAdjustedToUtc);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr TimeLogicalType_TimeUnit(IntPtr logicalType, out TimeUnit timeUnit);
    }

    public sealed class TimestampLogicalType : LogicalType
    {
        internal TimestampLogicalType(IntPtr handle) : base(handle) { }

        public bool IsAdjustedToUtc => ExceptionInfo.Return<bool>(Handle, TimestampLogicalType_IsAdjustedToUtc);
        public bool ForceSetConvertedType => ExceptionInfo.Return<bool>(Handle, TimestampLogicalType_ForceSetConvertedType);
        public bool IsFromConvertedType => ExceptionInfo.Return<bool>(Handle, TimestampLogicalType_IsFromConvertedType);
        public TimeUnit TimeUnit => ExceptionInfo.Return<TimeUnit>(Handle, TimestampLogicalType_TimeUnit);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr TimestampLogicalType_IsAdjustedToUtc(IntPtr logicalType, [MarshalAs(UnmanagedType.I1)] out bool isAdjustedToUtc);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr TimestampLogicalType_ForceSetConvertedType(IntPtr logicalType, [MarshalAs(UnmanagedType.I1)] out bool forceSetConvertedType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr TimestampLogicalType_IsFromConvertedType(IntPtr logicalType, [MarshalAs(UnmanagedType.I1)] out bool isFromConvertedType);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr TimestampLogicalType_TimeUnit(IntPtr logicalType, out TimeUnit timeUnit);
    }

    public sealed class IntervalLogicalType : LogicalType
    {
        internal IntervalLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class IntLogicalType : LogicalType
    {
        internal IntLogicalType(IntPtr handle) : base(handle) { }

        public int BitWidth => ExceptionInfo.Return<int>(Handle, IntLogicalType_BitWidth);
        public bool IsSigned => ExceptionInfo.Return<bool>(Handle, IntLogicalType_IsSigned);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr IntLogicalType_BitWidth(IntPtr logicalType, out int bitWidth);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr IntLogicalType_IsSigned(IntPtr logicalType, [MarshalAs(UnmanagedType.I1)] out bool isSigned);
    }

    public sealed class NullLogicalType : LogicalType
    {
        internal NullLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class JsonLogicalType : LogicalType
    {
        internal JsonLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class BsonLogicalType : LogicalType
    {
        internal BsonLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class UuidLogicalType : LogicalType
    {
        internal UuidLogicalType(IntPtr handle) : base(handle) { }
    }

    public sealed class NoneLogicalType : LogicalType
    {
        internal NoneLogicalType(IntPtr handle) : base(handle) { }
    }
}
