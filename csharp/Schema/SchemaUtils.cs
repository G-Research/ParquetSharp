namespace ParquetSharp.Schema
{
    internal static class SchemaUtils
    {
        public static bool IsListOrMap(Node[] schemaNodes)
        {
            if (schemaNodes.Length < 2)
            {
                return false;
            }

            // From https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types:
            // Lists:
            // - "The middle level, named list, must be a repeated group with a single field named element."
            //   The middle level being this.
            // - "The outer-most level must be a group annotated with LIST that contains a single field named list.
            //   The repetition of this level must be either optional or required and determines whether the list is nullable."
            // Maps:
            // - "The outer-most level must be a group annotated with MAP that contains a single field named key_value.
            //    The repetition of this level must be either optional or required and determines whether the list is nullable."
            // - "The middle level, named key_value, must be a repeated group with a key field for map keys and, optionally,
            //    a value field for map values."
            // - "The key field encodes the map's key type. This field must have repetition required and must always be present.
            //   The value field encodes the map's value type and repetition. This field can be required, optional, or omitted."
            var rootNode = schemaNodes[0];
            var childNode = schemaNodes[1];
            using var rootLogicalType = rootNode.LogicalType;
            using var childLogicalType = childNode.LogicalType;

            return rootNode is GroupNode &&
                   rootLogicalType is ListLogicalType or MapLogicalType &&
                   rootNode.Repetition is Repetition.Optional or Repetition.Required &&
                   childNode is GroupNode &&
                   childLogicalType is NoneLogicalType &&
                   childNode.Repetition is Repetition.Repeated;
        }
    }
}
