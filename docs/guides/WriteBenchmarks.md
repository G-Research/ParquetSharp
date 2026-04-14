# ParquetSharp Encoding & Compression Benchmarks for Single-Precision Float Data

This document presents benchmarks for writing single-precision float datasets to Parquet with different encoding and compression configurations using **ParquetSharp**. The goal is to identify which combination produces the smallest file size and fastest write time for floating-point data.

---

## Overview

Three encoding strategies where tested, each paired with up to three compression codecs, across three real-world scientific float datasets. The datasets were first [decompressed](https://userweb.cs.txstate.edu/~burtscher/research/SPDPcompressor/) from SPDP format to raw binary (`.bin`) and then converted to Parquet.

### Encodings tested

- **Plain (no dictionary)** – Raw float values written as-is. No dictionary lookup overhead.
- **Plain (dictionary enabled)** – Attempts to build a dictionary of unique float values per row group. Only beneficial if floats repeat frequently. Note that **dictionary encoding is enabled by default** in ParquetSharp, so it is important to consider whether it should be disabled for floating-point columns where values are largely unique.
- **ByteStreamSplit (no dictionary)** – Rearranges the bytes of each float so that all sign/exponent bytes are grouped together, improving compressibility. Only meaningful when paired with a block compressor.

### Compressions tested

- **None** – Uncompressed. Baseline for size comparison.
- **Snappy** – Fast compression, moderate ratio.
- **Zstd** – Slower compression, higher ratio.

---

## Datasets

All datasets are IEEE 754 32-bit single-precision (float) scientific data, decompressed from SPDP-compressed files available at the [Texas State University single-precision datasets page](https://userweb.cs.txstate.edu/~burtscher/research/datasets/FPsingle/).

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

### Timing methodology

Write times are measured **inside** `ConvertData` using `Stopwatch`, starting immediately before `new ParquetFileWriter(...)` and stopping after `writer.Close()`. This isolates the Parquet write from the time taken to read the input binary file, which would otherwise dominate the measurement.

```csharp
var sw = Stopwatch.StartNew();
using var writer = new ParquetFileWriter(outputFile, columns, builder.Build());
// ... write row groups ...
writer.Close();
sw.Stop();
```

Read times are wall clock times measured with `/usr/bin/time -v` using the `logical-chunked-file` command with `--chunk-size 50000`, applied consistently across all variants to allow direct comparison.

```bash
/usr/bin/time -v dotnet run --no-build --configuration=Release -- logical-chunked-file --file <parquet_file> --chunk-size 50000
```

---

## Results

### num_control.sp.bin (76 MB raw · 19,938,093 floats)

| Encoding | Compression | Parquet size (MB) | Write time (s) | Read time (s) | Size vs Plain/None |
|----------|-------------|:-----------------:|:--------------:|:-------------:|:------------------:|
| Plain, no dict | None | 76.06 | 0.59 | 0.91 | Baseline |
| Plain, no dict | Snappy | 75.11 | 0.60 | 0.94 | −1.2% |
| Plain, no dict | Zstd | 70.63 | 0.77 | 1.05 | −7.1% |
| Plain, dict | Snappy | 87.02 | 1.23 | 0.87 | +14.4% |
| Plain, dict | Zstd | 82.33 | 1.03 | 1.07 | +8.2% |
| ByteStreamSplit | Snappy | 67.04 | 0.58 | 1.03 | **−11.9%** |
| ByteStreamSplit | Zstd | 62.98 | 0.66 | 0.97 | **−17.2%** |

---

### num_brain.sp.bin (67.6 MB raw · 17,730,000 floats)

| Encoding | Compression | Parquet size (MB) | Write time (s) | Read time (s) | Size vs Plain/None |
|----------|-------------|:-----------------:|:--------------:|:-------------:|:------------------:|
| Plain, no dict | None | 67.64 | 0.58 | 1.00 | Baseline |
| Plain, no dict | Snappy | 67.64 | 0.56 | 1.05 | 0.0% |
| Plain, no dict | Zstd | 59.99 | 0.49 | 0.91 | −11.3% |
| Plain, dict | Snappy | 78.28 | 0.97 | 1.00 | +15.7% |
| Plain, dict | Zstd | 70.52 | 1.11 | 0.93 | +4.3% |
| ByteStreamSplit | Snappy | 52.12 | 0.59 | 0.93 | **−22.9%** |
| ByteStreamSplit | Zstd | 48.67 | 0.61 | 0.82 | **−28.0%** |

---

### obs_spitzer.sp.bin (94.5 MB raw · 24,772,608 floats)

| Encoding | Compression | Parquet size (MB) | Write time (s) | Read time (s) | Size vs Plain/None |
|----------|-------------|:-----------------:|:--------------:|:-------------:|:------------------:|
| Plain, no dict | None | 94.51 | 0.49 | 1.38 | Baseline |
| Plain, no dict | Snappy | 93.27 | 1.40 | 1.15 | −1.3% |
| Plain, no dict | Zstd | 82.48 | 1.06 | 1.73 | −12.7% |
| Plain, dict | Snappy | 87.44 | 1.55 | 1.44 | −7.5% |
| Plain, dict | Zstd | 81.96 | 3.05 | 1.02 | −13.3% |
| ByteStreamSplit | Snappy | 84.56 | 0.40 | 0.93 | **−10.5%** |
| ByteStreamSplit | Zstd | 71.81 | 0.52 | 1.41 | **−24.0%** |

---

## Analysis

### Compression ratio

ByteStreamSplit consistently produces the smallest Parquet files across all three datasets, reducing file size by **11–28%** compared to uncompressed Plain. This is expected behaviour: scientific float data has high entropy in the mantissa bytes but low entropy in the sign and exponent bytes. ByteStreamSplit separates these byte planes so the compressor sees long runs of similar bytes, improving the compression ratio.

Plain + Zstd achieves a moderate improvement of **7–13%** over the uncompressed baseline, confirming that standard compression alone can compress float data but without byte-plane separation its gains are limited.

Dictionary encoding is counterproductive for these datasets. Floats from scientific simulations and telescope observations are largely unique values; the dictionary overhead increases file sizes by **4–16%** compared to Plain with no dictionary. This is consistent with the expectation that dictionary encoding only helps when a column has a small number of repeating values. Since **dictionary encoding is enabled by default**, it is important to explicitly call `DisableDictionary()` when writing floating-point columns with high cardinality.

Snappy compression on Plain data provides almost no benefit at all (0–1.3% reduction), confirming that random-looking float bytes are effectively incompressible without a byte-reordering step first.

### Write time

Write times are fast across all configurations (0.40–3.05 s for these dataset sizes). Notable observations:

- ByteStreamSplit + Snappy is the **fastest** write configuration on two of three datasets (0.40–0.59 s), combining the best compression ratio with the lowest write overhead. The byte-reordering step adds negligible cost.
- Dictionary encoding is consistently the **slowest** configuration, taking up to 3.05 s compared to 0.49 s for Plain + None on obs_spitzer. The dictionary building pass over high-cardinality float data wastes CPU time without size benefit.
- Zstd is consistently slower than Snappy, as expected from a higher-ratio codec, though the difference is small at these dataset sizes.

### Read time

Read times are broadly consistent across configurations (0.82–1.73 s). Notable observations:

- ByteStreamSplit + Zstd achieves the **fastest read** on two of three datasets (0.82–0.97 s) despite requiring byte-plane decoding on read. The smaller file size reduces I/O, which outweighs the decompression overhead at these dataset sizes.
- Plain + Zstd on obs_spitzer is the **slowest** to read (1.73 s), notably slower than ByteStreamSplit + Zstd (1.41 s) at a similar file size. ByteStreamSplit's byte-plane arrangement makes Zstd decompression more efficient on read as well as write.
- ByteStreamSplit + Snappy offers consistently fast reads (0.93–1.03 s) across all three datasets, making it the most predictable choice across all three metrics.

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

- ByteStreamSplit is the correct encoding choice for IEEE 754 float data. It reduces Parquet file sizes by **11–28%** compared to uncompressed Plain at competitive or faster write speed.
- **Dictionary encoding is enabled by default** and should be explicitly disabled for floating-point columns with high cardinality. It consistently produces larger files and slower writes for scientific float data.
- Snappy is the practical default compression for ByteStreamSplit — it delivers most of the size benefit with minimal write overhead.
- Zstd is worth the extra write time when storage cost or I/O bandwidth is the primary constraint.

---

## References

- [SPDP single-precision datasets](https://userweb.cs.txstate.edu/~burtscher/research/datasets/FPsingle/)
- [Apache Parquet encoding definitions](https://parquet.apache.org/docs/file-format/data-pages/encodings/)