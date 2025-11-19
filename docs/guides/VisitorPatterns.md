# Visitor patterns: reading & writing with unknown column types

ParquetSharp exposes a number of "visitor" interfaces that make it convenient to read or write columns when you don't know the concrete column types at compile time. These visitors let you write type-safe code that is invoked for the actual column element type at runtime.

## ILogicalColumnWriterVisitor<TReturn>

The `ILogicalColumnWriterVisitor<TReturn>` interface is invoked for logical writers (high-level typed writers). Use this when you need to write data to columns but don't know the column types at compile time.

### Example: Generic column writer

```csharp
// A visitor that writes arrays of values to any column type
sealed class GenericColumnWriter : ILogicalColumnWriterVisitor<bool>
{
    private readonly IDictionary<string, object> _valuesByColumn;

    public GenericColumnWriter(IDictionary<string, object> valuesByColumn)
    {
        _valuesByColumn = valuesByColumn;
    }

    public bool OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
    {
        // Look up values for this column name
        if (!_valuesByColumn.TryGetValue(columnWriter.ColumnDescriptor.Path[0], out var raw))
            return false;

        // Cast through object to TValue[] for WriteBatch
        var values = (TValue[])(object)raw;
        columnWriter.WriteBatch(values);
        return true;
    }
}

// Usage
var valuesByColumn = new Dictionary<string, object>
{
    { "Id", new[] { 1, 2, 3 } },
    { "Name", new[] { "Alice", "Bob", "Carol" } },
    { "Price", new[] { 9.99, 12.50, 5.75 } }
};

using var logicalWriter = columnWriter.LogicalWriter();
var success = logicalWriter.Apply(new GenericColumnWriter(valuesByColumn));
```

### Example: Conditional writer based on type

```csharp
// A visitor that only writes numeric columns, skipping others
sealed class NumericOnlyWriter : ILogicalColumnWriterVisitor<bool>
{
    private readonly double _fillValue;

    public NumericOnlyWriter(double fillValue) => _fillValue = fillValue;

    public bool OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
    {
        // Only write if TValue is a numeric type
        if (typeof(TValue) == typeof(int) || typeof(TValue) == typeof(double) || 
            typeof(TValue) == typeof(float) || typeof(TValue) == typeof(long))
        {
            var values = new TValue[] { (TValue)(object)_fillValue };
            columnWriter.WriteBatch(values);
            return true;
        }
        return false;
    }
}
```

## ILogicalColumnReaderVisitor<TReturn>

The `ILogicalColumnReaderVisitor<TReturn>` interface is invoked for logical readers (high-level typed readers). Use this when you need to read data from columns of unknown types.

### Example: Convert columns to strings

```csharp
// A visitor that reads all values and returns them as a comma-separated string
sealed class ColumnToStringReader : ILogicalColumnReaderVisitor<string>
{
    public string OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
    {
        var sb = new StringBuilder();
        const int bufferSize = 1024;
        var buffer = new TElement[bufferSize];

        while (columnReader.HasNext)
        {
            var read = columnReader.ReadBatch(buffer);
            for (var i = 0; i < read; ++i)
            {
                var v = buffer[i];
                sb.Append(v?.ToString() ?? "null");
                sb.Append(", ");
            }
        }

        if (sb.Length >= 2) sb.Length -= 2;
        return sb.ToString();
    }
}

// Usage
using var logicalReader = columnReader.LogicalReader();
var columnString = logicalReader.Apply(new ColumnToStringReader());
Console.WriteLine($"Column data: {columnString}");
```

### Example: Calculate column statistics

```csharp
// A visitor that computes row count for any column type
sealed class RowCountReader : ILogicalColumnReaderVisitor<long>
{
    public long OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
    {
        long count = 0;
        const int bufferSize = 1024;
        var buffer = new TElement[bufferSize];

        while (columnReader.HasNext)
        {
            var read = columnReader.ReadBatch(buffer);
            count += read;
        }

        return count;
    }
}

// Usage
using var logicalReader = columnReader.LogicalReader();
var rowCount = logicalReader.Apply(new RowCountReader());
Console.WriteLine($"Total rows: {rowCount}");
```

## IColumnWriterVisitor<TReturn>

The `IColumnWriterVisitor<TReturn>` interface provides lower-level access to physical column writers. Use this when you need to work with physical types, definition levels, repetition levels, or encodings.

### Example: Physical type inspector

```csharp
// A visitor that reports the physical type being written
sealed class PhysicalTypeWriter : IColumnWriterVisitor<string>
{
    public string OnColumnWriter<TValue>(ColumnWriter<TValue> columnWriter) 
        where TValue : unmanaged
    {
        var physicalType = typeof(TValue).Name;
        Console.WriteLine($"Writing physical type: {physicalType}");
        
        // Could perform low-level writes here if needed
        // columnWriter.WriteBatch(..., definitionLevels, repetitionLevels);
        
        return physicalType;
    }
}
```

## IColumnReaderVisitor<TReturn>

The `IColumnReaderVisitor<TReturn>` interface provides lower-level access to physical column readers. Use this for low-level operations that require access to definition levels, repetition levels, or physical encodings.

### Example: Definition level analyzer

```csharp
// A visitor that counts null values using definition levels
sealed class NullCountReader : IColumnReaderVisitor<int>
{
    public int OnColumnReader<TValue>(ColumnReader<TValue> columnReader) 
        where TValue : unmanaged
    {
        const int bufferSize = 1024;
        var values = new TValue[bufferSize];
        var defLevels = new short[bufferSize];
        var repLevels = new short[bufferSize];
        int nullCount = 0;

        while (columnReader.HasNext)
        {
            var read = columnReader.ReadBatch(bufferSize, defLevels, repLevels, values, out var valuesRead);
            
            // Count definition levels that indicate null
            for (int i = 0; i < read; i++)
            {
                if (defLevels[i] < columnReader.ColumnDescriptor.MaxDefinitionLevel)
                {
                    nullCount++;
                }
            }
        }

        return nullCount;
    }
}
```

## IColumnDescriptorVisitor<TReturn>

The `IColumnDescriptorVisitor<TReturn>` interface visits column descriptors (schema metadata) without performing any I/O. Use this when you only need to inspect or process schema information.

### Example: Schema type reporter

```csharp
// A visitor that generates a human-readable type description
sealed class TypeDescriptionVisitor : IColumnDescriptorVisitor<string>
{
    public string OnColumnDescriptor<TValue>(ColumnDescriptor<TValue> descriptor) 
        where TValue : unmanaged
    {
        var logicalType = descriptor.LogicalType?.ToString() ?? "none";
        var physicalType = typeof(TValue).Name;
        var repetition = descriptor.MaxRepetitionLevel > 0 ? "repeated" : "required";
        
        return $"{descriptor.Name}: {logicalType} (physical: {physicalType}, {repetition})";
    }
}

// Usage
var description = columnDescriptor.Apply(new TypeDescriptionVisitor());
Console.WriteLine(description);
```

### Example: Schema validator

```csharp
// A visitor that checks if a column meets certain requirements
sealed class SchemaValidator : IColumnDescriptorVisitor<bool>
{
    private readonly HashSet<string> _allowedNames;

    public SchemaValidator(HashSet<string> allowedNames)
    {
        _allowedNames = allowedNames;
    }

    public bool OnColumnDescriptor<TValue>(ColumnDescriptor<TValue> descriptor) 
        where TValue : unmanaged
    {
        // Check if column name is in allowed list
        if (!_allowedNames.Contains(descriptor.Name))
            return false;

        // Check if nullable when it shouldn't be
        if (descriptor.MaxDefinitionLevel > 0)
        {
            Console.WriteLine($"Warning: Column {descriptor.Name} is nullable");
        }

        return true;
    }
}
```

## Complete working example

Here's a full example demonstrating `ILogicalColumnWriterVisitor` and `ILogicalColumnReaderVisitor` together:

```csharp
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ParquetSharp;

namespace ParquetSharp.Examples
{
    sealed class ExampleWriter : ILogicalColumnWriterVisitor<bool>
    {
        private readonly IDictionary<string, object> _valuesByColumn;

        public ExampleWriter(IDictionary<string, object> valuesByColumn)
        {
            _valuesByColumn = valuesByColumn;
        }

        public bool OnLogicalColumnWriter<TValue>(LogicalColumnWriter<TValue> columnWriter)
        {
            if (!_valuesByColumn.TryGetValue(columnWriter.ColumnDescriptor.Path[0], out var raw))
                return false;

            var values = (TValue[])(object)raw;
            columnWriter.WriteBatch(values);
            return true;
        }
    }

    sealed class ExampleReader : ILogicalColumnReaderVisitor<string>
    {
        public string OnLogicalColumnReader<TElement>(LogicalColumnReader<TElement> columnReader)
        {
            var sb = new StringBuilder();
            const int bufferSize = 1024;
            var buffer = new TElement[bufferSize];

            while (columnReader.HasNext)
            {
                var read = columnReader.ReadBatch(buffer);
                for (var i = 0; i < read; ++i)
                {
                    var v = buffer[i];
                    sb.Append(v?.ToString() ?? "null");
                    sb.Append(", ");
                }
            }

            if (sb.Length >= 2) sb.Length -= 2;
            return sb.ToString();
        }
    }

    class VisitorReadWriteExample
    {
        public static void Run(string path)
        {
            var columns = new Column[]
            {
                new Column<int>("Id"),
                new Column<string>("Name"),
                new Column<DateTime>("Timestamp")
            };

            var ids = new[] { 1, 2, 3 };
            var names = new[] { "Alice", "Bob", "Carol" };
            var times = new[] { DateTime.UtcNow, DateTime.UtcNow.AddMinutes(1), DateTime.UtcNow.AddMinutes(2) };

            using (var writer = new ParquetFileWriter(path, columns))
            {
                using var rowGroup = writer.AppendRowGroup();
                for (int i = 0; i < columns.Length; ++i)
                {
                    using var colWriter = rowGroup.NextColumn();
                    var valuesByColumn = new Dictionary<string, object>
                    {
                        { "Id", ids },
                        { "Name", names },
                        { "Timestamp", times }
                    };

                    using var logicalWriter = colWriter.LogicalWriter();
                    logicalWriter.Apply(new ExampleWriter(valuesByColumn));
                }
                writer.Close();
            }

            using var fileReader = new ParquetFileReader(path);
            using var rg = fileReader.RowGroup(0);
            for (int i = 0; i < fileReader.FileMetaData.NumColumns; ++i)
            {
                using var colReader = rg.Column(i);
                using var logicalReader = colReader.LogicalReader();
                var colString = logicalReader.Apply(new ExampleReader());
                Console.WriteLine($"Column {colReader.ColumnDescriptor.Path[0]}: {colString}");
            }
        }
    }
}
```

## Best practices

### When to use each visitor type

- **ILogicalColumnWriterVisitor / ILogicalColumnReaderVisitor**: Use for high-level, type-safe reading and writing when column types are unknown at compile time. Ideal for generic tooling, schema-driven processing, and data exporters.

- **IColumnWriterVisitor / IColumnReaderVisitor**: Use for low-level operations requiring access to definition levels, repetition levels, or physical encodings. Needed for nested types and null handling.

- **IColumnDescriptorVisitor**: Use when you only need to inspect schema metadata without performing I/O. Perfect for schema validation, type checking, and metadata extraction.

### When to avoid visitors

If you already know the schema at compile time, prefer the generic `LogicalWriter<T>` / `LogicalReader<T>` APIs — they are simpler, faster, and more maintainable.

### Casting arrays safely

The `(TValue[])(object)array` cast pattern is safe when the visitor is invoked with the concrete `TValue` type that matches your stored array element type. Always ensure your stored arrays match the declared column types to avoid runtime exceptions.