namespace ParquetSharp
{
    internal enum BinaryType
    {
        /// <summary>
        /// Variable-length bytes (no guarantee of UTF8-ness)
        /// </summary>
        Binary = 14,

        /// <summary>
        /// Like BINARY, but with 64-bit offsets
        /// </summary>
        LargeBinary = 35,

        /// <summary>
        /// Bytes view type with 4-byte prefix and inline small string optimization
        /// </summary>
        BinaryView = 40
    }
}