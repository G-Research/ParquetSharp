#!/usr/bin/env python3

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# These assertions document which versions were used to generate the test data.
assert pd.__version__ == "1.4.3"
assert pa.__version__ == "8.0.0"

nested_structure = '''
[
    {
        "first_level_long": 1,
        "first_level_nullable_string": null,
        "nullable_struct": {
            "nullable_struct_string": "Nullable Struct String"
        },
        "struct": {
            "struct_string": "First Struct String"
        },
        "struct_array": [
            {
                "string_in_struct_array": "First String",
                "array_in_struct_array": [111, 112, 113]
            },
            {
                "string_in_struct_array": "Second String",
                "array_in_struct_array": [121, 122, 123]
            }
        ]
    },
    {
        "first_level_long": 2,
        "first_level_nullable_string": "Not Null String",
        "nullable_struct": null,
        "struct": {
            "struct_string": "Second Struct String"
        },
        "struct_array": [
            {
                "string_in_struct_array": "Third String",
                "array_in_struct_array": [211, 212, 213]
            }
        ]
    }
]
'''

pq.write_table(
    pa.Table.from_pandas(
        pd.io.json.read_json(nested_structure),
        preserve_index=False
    ),
    'nested.parquet',
    version='2.0'
)

pq.write_table(
    pa.Table.from_pandas(
        pd.DataFrame({
            'col1': pd.Series([
                [('key1', 'aaaa'), ('key2', 'bbbb')],
                [('key3', '1111'), ('key4', '2222')],
            ]),
            'col2': pd.Series(['foo', 'bar'])
        }),
        schema=pa.schema([pa.field('col1', pa.map_(pa.string(), pa.string())), pa.field('col2', pa.string())])
    ),
    'map.parquet'
)
