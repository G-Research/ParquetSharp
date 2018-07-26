
namespace ParquetSharp
{
    public enum LogicalType
    {
        None = 0,
        Utf8 = 1,
        Map = 2,
        MapKeyValue = 3,
        List = 4,
        Enum = 5,
        Decimal = 6,
        Date = 7,
        TimeMillis = 8,
        TimeMicros = 9,
        TimestampMillis = 10,
        TimestampMicros = 11,
        UInt8 = 12,
        UInt16 = 13,
        UInt32 = 14,
        UInt64 = 15,
        Int8 = 16,
        Int16 = 17,
        Int32 = 18,
        Int64 = 19,
        Json = 20,
        Bson = 21,
        Interval = 22,
        NA = 25
    };
}