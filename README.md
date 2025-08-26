# Parquet output plugin for Embulk

embulk-output-parquet is an Embulk output plugin that writes Parquet files to the local filesystem.

## Overview
Required Embulk version >= 0.11.5.
Java 11+.

* **Plugin type**: output
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration
This plugin writes Parquet files only to the local filesystem.

### Embulk Configuration
- **output_dir**: local directory to write Parquet files. (string, required)
- **file_prefix**: file name prefix (e.g., "part"). (string, optional, default: "part")
- **compression**: Parquet compression codec (e.g., "SNAPPY", "GZIP"). (string, optional, default: "SNAPPY")

### Notes
- Timestamps are stored as Avro logical type `timestamp-micros` (epoch micros).
- JSON columns are not supported.

## Example
Example is written by maven style. rubygem style is also available.

1. Write Parquet files locally.
```yaml
out: 
  type:
    source: maven
    group: io.github.shin1103 
    name: parquet 
    version: 0.2.0
  output_dir: "/path/to/output"
  file_prefix: "part"
  compression: "SNAPPY"
```
 
## Supported Types
- boolean, long, double, string, timestamp

Unsupported: json
