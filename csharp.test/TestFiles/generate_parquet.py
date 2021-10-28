#!/usr/bin/env python3

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# pd.__version__ == 1.3.3
# pa.__version__ == 5.0.0

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

df = pd.io.json.read_json(nested_structure)
parquetTable = pa.Table.from_pandas(df, preserve_index=False)
print(parquetTable.schema)
pq.write_table(parquetTable, 'nested.parquet', version='2.0')