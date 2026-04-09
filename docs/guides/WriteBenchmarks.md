# ParquetSharp Encoding & Compression Benchmarks for Single-Precision Float Data

This document presents benchmarks for writing single-precision float datasets to Parquet with different encoding and compression configurations using **ParquetSharp**. The goal is to identify which combination produces the smallest file size and fastest write time for floating-point data.

---

## Overview

Three encoding strategies where tested, each paired with up to three compression codecs, across three real-world scientific float datasets. The datasets were first [decompressed](https://userweb.cs.txstate.edu/~burtscher/research/SPDPcompressor/) from SPDP format to raw binary (`.bin`) and then converted to Parquet.

### Encodings tested

- **Plain (no dictionary)** – Raw float values written as-is. No dictionary lookup overhead.
- **Plain (dictionary enabled)** – Attempts to build a dictionary of unique float values per row group. Only beneficial if floats repeat frequently.
- **ByteStreamSplit (no dictionary)** – Rearranges the bytes of each float so that all sign/exponent bytes are grouped together, improving compressibility. Only meaningful when paired with a block compressor.

### Compressions tested

- **None** – Uncompressed. Baseline for size comparison.
- **Snappy** – Fast compression, moderate ratio.
- **Zstd** – Slower compression, higher ratio.

---

## Datasets

All datasets are IEEE 754 32-bit single-precision (float) scientific data, decompressed from SPDP-compressed files.

| Dataset | Source binary | Raw size | Float count |
|---------|--------------|----------|-------------|
| num_control | num_control.sp.bin | 76 MB | 19,938,093 |
| num_brain | num_brain.sp.bin | 67.6 MB | 17,730,000 |
| obs_spitzer | obs_spitzer.sp.bin | 94.5 MB | 24,772,608 |

---

## Test Environment

- **OS:** Ubuntu 24.04 LTS
- **Execution:** VMware virtual machine
- **RAM:** 32 GB
- **CPU:** 2 vCPUs
- **Storage:** Local file system
- **Runtime:** `dotnet run --no-build --configuration=Release`
- **Timing:** `/usr/bin/time -v`

---

## Results

### num_control.sp.bin (76 MB raw · 19,938,093 floats)

| Encoding | Compression | Parquet size (MB) | Wall time (s) | Peak RSS (MB) | Size vs Plain/None |
|----------|-------------|:-----------------:|:-------------:|:-------------:|:------------------:|
| Plain, no dict | None | 152.13 | 2.27 | 366 | Baseline |
| Plain, no dict | Snappy | 151.18 | 2.20 | 368 | −0.6% |
| Plain, no dict | Zstd | 121.31 | 2.46 | 368 | −20.3% |
| Plain, dict | Snappy | 174.35 | 3.89 | 389 | +14.6% |
| Plain, dict | Zstd | 143.60 | 3.35 | 389 | −5.6% |
| ByteStreamSplit | Snappy | 70.84 | 2.44 | 368 | **−53.4%** |
| ByteStreamSplit | Zstd | 63.16 | 3.43 | 369 | **−58.5%** |

---

### num_brain.sp.bin (67.6 MB raw · 17,730,000 floats)

| Encoding | Compression | Parquet size (MB) | Wall time (s) | Peak RSS (MB) | Size vs Plain/None |
|----------|-------------|:-----------------:|:-------------:|:-------------:|:------------------:|
| Plain, no dict | None | 135.28 | 2.37 | 332 | Baseline |
| Plain, no dict | Snappy | 135.29 | 2.68 | 334 | 0.0% |
| Plain, no dict | Zstd | 105.02 | 3.06 | 334 | −22.4% |
| Plain, dict | Snappy | 156.06 | 3.32 | 355 | +15.4% |
| Plain, dict | Zstd | 125.09 | 2.66 | 355 | −7.5% |
| ByteStreamSplit | Snappy | 55.50 | 2.08 | 335 | **−59.0%** |
| ByteStreamSplit | Zstd | 48.83 | 1.97 | 336 | **−63.9%** |

---

### obs_spitzer.sp.bin (94.5 MB raw · 24,772,608 floats)

| Encoding | Compression | Parquet size (MB) | Wall time (s) | Peak RSS (MB) | Size vs Plain/None |
|----------|-------------|:-----------------:|:-------------:|:-------------:|:------------------:|
| Plain, no dict | None | 189.02 | 2.31 | 440 | Baseline |
| Plain, no dict | Snappy | 187.78 | 2.77 | 442 | −0.7% |
| Plain, no dict | Zstd | 145.49 | 2.75 | 443 | −23.0% |
| Plain, dict | Snappy | 196.03 | 4.47 | 465 | +3.7% |
| Plain, dict | Zstd | 158.22 | 3.47 | 466 | −16.3% |
| ByteStreamSplit | Snappy | 89.28 | 1.79 | 442 | **−52.8%** |
| ByteStreamSplit | Zstd | 72.03 | 2.07 | 443 | **−61.9%** |

---

## Analysis

### Compression ratio

ByteStreamSplit consistently produces the smallest Parquet files across all three datasets, reducing file size by **53–64%** compared to uncompressed Plain. This is expected behaviour: scientific float data has high entropy in the mantissa bytes but low entropy in the sign and exponent bytes. ByteStreamSplit separates these byte planes so the compressor sees long runs of similar bytes, dramatically improving the compression ratio.

Plain + Zstd achieves a moderate improvement of **20–23%** over the uncompressed baseline, confirming that standard compression alone can compress float data but without byte-plane separation its gains are limited.

Dictionary encoding is counterproductive for these datasets. Floats from scientific simulations and telescope observations are largely unique values; the dictionary overhead increases file sizes by **4–15%** compared to Plain with no dictionary. This is consistent with the expectation that dictionary encoding only helps when a column has a small number of repeating values.

Snappy compression on Plain data provides almost no benefit at all (less than 1% reduction), confirming that random-looking float bytes are effectively incompressible without a byte-reordering step first.

### Write time

Wall times are broadly consistent across configurations (1.8–4.5 s for these dataset sizes). Notable observations:

- ByteStreamSplit + Snappy is the **fastest** write configuration on two of three datasets, combining the best compression ratio with competitive write speed. The byte-reordering step adds negligible overhead.
- Dictionary encoding is the **slowest** configuration, taking up to 4.47 s compared to 2.31 s for Plain + None on obs_spitzer. The dictionary building pass over high-cardinality float data wastes CPU time without size benefit.
- Zstd is consistently slower than Snappy, as expected from a higher-ratio codec.

### Memory (Peak RSS)

Peak resident set size is driven almost entirely by the float array loaded from the binary file, not by the encoding or compression choice. All configurations land within approximately 10–25 MB of each other per dataset. The dictionary configurations show a modest increase (~20–25 MB) due to the in-memory dictionary structure.

---

## Summary

| Encoding | Compression | Recommended for |
|----------|-------------|-----------------|
| ByteStreamSplit | Zstd | **Smallest files.** Best overall if write throughput allows slightly longer write time. |
| ByteStreamSplit | Snappy | **Best balance.** Fastest writes with near-optimal compression for float data. |
| Plain, no dict | Zstd | Moderate size reduction. Compatible with tools that do not support ByteStreamSplit. |
| Plain, no dict | None | Maximum compatibility, fastest read, largest files. |
| Plain, dict | any | **Not recommended** for scientific float data — floats have too many unique values. |

### Key findings

- ByteStreamSplit is the correct encoding choice for IEEE 754 float data. It reduces Parquet file sizes by **53–64%** compared to uncompressed Plain at no measurable memory cost and competitive write speed.
- Dictionary encoding should be disabled for scientific float datasets. It consistently produces larger files and slower writes.
- Snappy is the practical default compression for ByteStreamSplit — it delivers most of the size benefit with minimal write overhead.
- Zstd is worth the extra write time when storage cost or I/O bandwidth is the primary constraint.

---

## Throughput Calculation

Throughput is calculated based on the raw (uncompressed) float data size.

Per-row size: `float` = 4 bytes  

```
RawDataSize = FloatCount × 4 bytes
```

| Dataset | Float count | Raw size |
|---------|------------|----------|
| num_control | 19,938,093 | ~76 MB |
| num_brain | 17,730,000 | ~67.6 MB |
| obs_spitzer | 24,772,608 | ~94.5 MB |

```
Throughput (MB/s) = RawDataSize (MB) / Wall time (s)
```

---

## References

- [SPDP Datasets](https://userweb.cs.txstate.edu/~burtscher/research/datasets/FPsingle/)
- [Apache Parquet encoding definitions](https://parquet.apache.org/docs/file-format/data-pages/encodings/)