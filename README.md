# FastGrep

A Python-native, multi-core, compressed-file-friendly grep replacement for large-scale text search.

## 🔧 Features

- ✅ Multi-core processing using `concurrent.futures`
- ✅ Supports `.gz`, `.bz2`, and regular text files
- ✅ Processes directories recursively
- ✅ Clean CLI with `argparse`
- ✅ Smart chunked reads for speed on big files

## 🚀 Usage

```bash
Usage: FastGrep.py [OPTIONS] <pattern> <file1.bz2> [file2.bz2 ...]

Search for a regex pattern inside compressed .bz2 files using parallel processing.

Options:
  -i, --ignore-case      Perform a case-insensitive search.
  -r, --recursive        Search .bz2 files in directories recursively.
  -c, --count            Print the number of matching lines per file.
  -o, --output <file>    Save search results to a specified file.
  -w, --workers <N>      Set the number of CPU cores to use (default: auto).
  -h, --help             Show this help message and exit.

Examples:
  FastGrep.py "error" logs1.bz2 logs2.bz2
  FastGrep.py -i "failure" logs_*.bz2
  FastGrep.py -o results.txt "critical failure" logs_*.bz2

