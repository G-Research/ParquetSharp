namespace ParquetSharp.Schema
{
    public enum ConvertedType
    {
        None = 0,  // Not a real converted type, but means no converted type is specified
        UTF8 = 1,
        Map = 2,
        MapKeyValue = 3,
        List = 4,
        Enum = 5,
        Decimal = 6,
        Date = 7,
        Time_Millis = 8,
        Time_Micros = 9,
        Timestamp_Millis = 10,
        Timestamp_Micros = 11,
        UInt_8 = 12,
        UInt_16 = 13,
        UInt_32 = 14,
        UInt_64 = 15,
        Int_8 = 16,
        Int_16 = 17,
        Int_32 = 18,
        Int_64 = 19,
        Json = 20,
        Bson = 21,
        Interval = 22,
        // DEPRECATED INVALID ConvertedType for all-null data.
        // Only useful for reading legacy files written out by interim Parquet C++ releases.
        // For writing, always emit LogicalType::Null instead.
        // See PARQUET-1990.
        NA = 25,
        Undefined = 26  // Not a real converted type; should always be last element
    }
}
