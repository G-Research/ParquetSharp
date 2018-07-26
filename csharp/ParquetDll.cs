
namespace ParquetSharp
{
    internal static class ParquetDll
    {
#if DEBUG
        public const string Name = "ParquetSharpNatived.dll";
#else
        public const string Name = "ParquetSharpNative.dll";
#endif
    }

}