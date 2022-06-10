#!/usr/bin/env python3

import string
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


random.seed(0)


def generate_string(num_letters):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(num_letters))


def numeric_string_array():
    return [generate_string(random.randint(50, 100)) for i in range(1, 100)]


def generate_data(filename):
    records = [{'1': [{'2': numeric_string_array()}]} for i in range(1, 1000)]
    df = pd.DataFrame.from_records(records)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, filename)


generate_data('nested_string_arrays.parquet')
