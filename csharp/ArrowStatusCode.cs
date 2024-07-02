namespace ParquetSharp
{
    /// <summary>
    /// Subset of Arrow StatusCode enums used from ParquetSharp.
    /// </summary>
    internal enum ArrowStatusCode
    {
        OutOfMemory = 1,
        IOError = 5,
        UnknownError = 9,
    }
}
