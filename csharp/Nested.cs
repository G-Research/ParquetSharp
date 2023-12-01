namespace ParquetSharp
{
    /// <summary>
    /// Wraps values that are nested inside group nodes in the Parquet schema,
    /// so that complex nested structures can be interpreted from columnar arrays.
    /// </summary>
    /// <typeparam name="T">The type of the inner nested value</typeparam>
    public readonly struct Nested<T>
    {
        public readonly T Value;

        public Nested(T value)
        {
            Value = value;
        }
    }
}
