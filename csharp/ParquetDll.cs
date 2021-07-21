namespace ParquetSharp
{
    internal static class ParquetDll
    {
#if DEBUG
        public const string Name = "ParquetSharpNatived";
#else
        public const string Name = "ParquetSharpNative";
#endif
    }
}
