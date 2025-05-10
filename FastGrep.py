#!/usr/bin/python

#
#     Title    : FastGrep.py
#     Version  : 1.0
#     Date     : 22 February 2025
#     Author   : Daniel Gavin
#
#     Function : A speedy port of grep and varients zgrep and bzgrep.
#
#     Modification History
#
#     Date     : 22 February 2025
#     Author   : Daniel Gavin
#     Changes  : New file.
#
#     Date     :
#     Author   :
#     Changes  :
#

import os
import re
import bz2
import sys
import gzip
import argparse
from concurrent.futures import ProcessPoolExecutor

# in bytes : 32KB  -> 32768
#          : 64KB  -> 65536
#          : 128KB -> 131072
#          : 256KB -> 262144

CHUNK_SIZE = 65536

###############################################################################
#
# Procedure   : ParseArgs()
#             
# Description : Handles argument parsing, and returns options.
#
# Input       : -none- 
#
# Returns     : argparse object.  
#             
###############################################################################

def ParseArgs():

    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument('pattern', nargs='?',  help='Regex pattern to search for.')
    parser.add_argument('files',   nargs='*',  help='Files or directories to search in.')
    parser.add_argument('-i', '--ignore-case', action='store_true', help='Ignore case distinctions.')
    parser.add_argument('-r', '--recursive',   action='store_true', help='Recurse into directories.')
    parser.add_argument('-c', '--count',       action='store_true', help='Show number of matching lines per file.')
    parser.add_argument('-o', '--output',      type=str, help='Write output to a file instead of stdout.')
    parser.add_argument('-w', '--workers',     type=int, default=max(1, os.cpu_count() - 1), help='Set number of worker cores.')
    parser.add_argument('-h', '--help',        action='store_true', help='Show this help message and exit.')
    parser.add_argument('-v', '--version',     action='version', version='SuperGrep v1.0')

    return parser.parse_args()


###############################################################################
#
# Procedure   : DisplayHelp()
#
# Description : Prints the "--help" text to the screen.
#
# Input       : -none-
#
# Returns     : -none-
#
###############################################################################

def DisplayHelp():

    print('Usage: FastGrep.py [OPTIONS] <pattern> <file1.bz2> [file2.bz2 ...]')
    print('')
    print('Search for a regex pattern inside compressed .bz2 files using parallel processing.')
    print('')
    print('Options:')
    print('  -i, --ignore-case      Perform a case-insensitive search.')
    print('  -r, --recursive        Search .bz2 files in directories recursively.')
    print('  -c, --count            Print the number of matching lines per file.')
    print('  -o, --output <file>    Save search results to a specified file.')
    print('  -w, --workers <N>      Set the number of CPU cores to use (default: auto).')
    print('  -h, --help             Show this help message and exit.')
    print('')
    print('Examples:')
    print('  FastGrep.py "error" logs1.bz2 logs2.bz2')
    print('  FastGrep.py -i "failure" logs_*.bz2')
    print('  FastGrep.py -o results.txt "critical failure" logs_*.bz2')


###############################################################################
#     
# Procedure   : DetectFileType()
#
# Description : Determines file type based on filename extension.
#             : - Recognizes .bz2, .gz, and .txt file types.
#             : - Returns 'unknown' unrecognized types.
#
# Input       : filename - file name to evaluate.
#     
# Returns     : 'bz'      - bzip2 compressed
#             : 'gz'      - gzip compressed
#             : 'txt'     - default
#     
###############################################################################

def DetectFileType(filename):

    if filename.endswith('.bz2'):
        return 'bz'

    elif filename.endswith('.gz'):
        return 'gz'

    else:
        return 'txt'


###############################################################################
#
# Procedure   : SearchFilesInParallel()
# 
# Description : Execute parallel regex search across files.
#             : Supports match-count and full-line return.
#
#             : If searching two files and have two "buffPonies", both files
#             : will be searched at the ame time on cores
# 
# Input       : func        - Search function to execute (bz2/gz/flat).
#             : files       - List of files to search.
#             : pattern     - Regex pattern to search for.
#             : ignoreCase  - Case ignore flag.
#             : countOnly   - True=return match count of file.
#             : buffPonies  - Max number of parallel workers.
#   
# Returns     : List of match strings or match counts.
#   
###############################################################################
    
def SearchFilesInParallel(func, files, pattern, ignoreCase, countOnly, buffPonies):
    
    retVal = []
    
    with ProcessPoolExecutor(max_workers=buffPonies) as executor:
        results = executor.map(func, files, [pattern] * len(files), [ignoreCase] * len(files))
    
        for matches in results:
    
            if matches:
                if countOnly:
                    retVal.append(f"{matches[0].split(':')[0]}:{len(matches)}")

                else:
                    retVal.extend(matches)

    return retVal


###############################################################################
#
# Procedure   : SearchInFlat()
#
# Description : Reads plain text files and searches for regex pattern.
#             : - Uses chunked buffered reads for performance.
#             : - Splits on newline and preserves partials between chunks.
#             : - Returns a list of matching lines.
#
# Input       : filename     - Path to plain text file.
#             : pattern      - Regex string to match inside file content.
#             : ignore_case  - Boolean flag for case-insensitive search.
#
# Returns     : List of matching lines.
#
###############################################################################

def SearchInFlat(filename, pattern, ignore_case=False):

    retVal = []

    if not os.path.isfile(filename):
        print('[ERROR] File not found: ' + filename)
        return []

    flags           = re.IGNORECASE if ignore_case else 0
    compiledPattern = re.compile(pattern.encode(), flags)

    try:
        with open(filename, 'rb') as infile:

            buffer = b""
            chunk  = infile.read(CHUNK_SIZE)

            while chunk:

                buffer += chunk
                lines   = buffer.split(b"\n")
                buffer  = lines.pop()

                for line in lines:
                    if compiledPattern.search(line):
                        retVal.append(f"{filename}:{line.decode(errors='ignore').strip()}")

                chunk = infile.read(CHUNK_SIZE)

    except Exception as e:
        retVal.append(f"[ERROR] Could not read {filename}: {e}")

    return retVal


###############################################################################
#   
# Procedure   : SearchInBz2()
#
# Description : Reads compressed .bz2 files and searches for regex pattern.
#             : - Speed things up using streaming decompression.
#             : - Chunk buffer processing for speed
#             : - Returns a list of matching lines.
#       
# Input       : filename - path to the .bz2 file.
#             : pattern  - regex string to match inside file content.
#   
# Returns     : List of matching lines. 
#   
###############################################################################

def SearchInBz2(filename, pattern, ignore_case=False):

    retVal = []

    if not os.path.isfile(filename):
        print('[ERROR] File not found: ' + filename)
        return []

    flags           = re.IGNORECASE if ignore_case else 0
    compiledPattern = re.compile(pattern.encode(), flags)
    decompressor    = bz2.BZ2Decompressor()

    try:
        with open(filename, "rb") as infile:

            buffer = b""
            chunk  = infile.read(CHUNK_SIZE)

            while chunk:

                buffer += decompressor.decompress(chunk)
                lines   = buffer.split(b"\n")
                buffer  = lines.pop()

                for line in lines:
                    if compiledPattern.search(line):
                        retVal.append(f"{filename}:{line.decode(errors='ignore').strip()}")

                chunk = infile.read(CHUNK_SIZE)

    except Exception as e:
        retVal.append(f"[ERROR] Could not read {filename}: {e}")

    return retVal


###############################################################################
#
# Procedure   : SearchInGz()
# 
# Description : Reads compressed .gz files and searches for regex pattern.
#             : - Uses streaming decompression for minimal memory usage.
#             : - Processes file line-by-line for efficiency.
#             : - Returns a list of matching lines with filename and line number.
#   
# Input       : filename - Path to the .gz file.
#             : pattern  - Regex string to match inside file content.
#
# Returns     : List of matching lines.
#   
###############################################################################

def SearchInGz(filename, pattern, ignore_case=False):

    retVal = []

    if not os.path.isfile(filename):
        print('[ERROR] File not found: ' + filename)
        return []

    flags           = re.IGNORECASE if ignore_case else 0
    compiledPattern = re.compile(pattern.encode(), flags)

    try:

        with open(filename, 'rb') as infile:

            with gzip.GzipFile(fileobj=infile) as gzfile:

                buffer = b""
                chunk = gzfile.read(CHUNK_SIZE)

                while chunk:

                    buffer += chunk
                    lines = buffer.split(b'\n')
                    buffer = lines.pop()

                    for line in lines:
                        if compiledPattern.search(line):
                            retVal.append(f"{filename}:{line.decode(errors='ignore').strip()}")

                    chunk = gzfile.read(CHUNK_SIZE)

    except Exception as e:
        retVal.append(f"[ERROR] Could not read {filename}: {e}")

    return retVal


###############################################################################
#
# Procedure   : FastFlatGrep()
# 
# Description : Searches for regex pattern inside plain text files.
#             : - Uses aggressive parallel processing.
#             : - Uses ProcessPoolExecutor for true multi-core execution.
#             : - Dispatches each file to a separate core for scanning.
#   
# Input       : files   - List of flat (uncompressed) text files to search.
#             : pattern - Regex string to match inside file content.
#
# Returns     : -none-
#   
###############################################################################

def FastFlatGrep(files, pattern, ignore_case):

    buffPonies = max(1, os.cpu_count() - 1)
    validFiles = [file for file in files if os.path.isfile(file)]

    if not validFiles:
        print("[INFO] No flat text files to process.")
        return

    with ProcessPoolExecutor(max_workers=buffPonies) as executor:
        results = executor.map(SearchInFlat, validFiles, [pattern] * len(validFiles), [ignore_case] * len(validFiles))

    for matchList in results:
        if matchList:
            sys.stdout.write("\n".join(matchList) + "\n")


###############################################################################
#
# Procedure   : FastBzgrep()
#
# Description : Searches for regex pattern inside compressed .bz2 files.
#             : - Uses aggressive parallel processing.
#             : - Uses ProcessPoolExecutor for true multi-core execution.
#   
# Input       : files   - List of .bz2 files to search.
#             : pattern - Regex string to match inside files.
#
# Returns     : -none-
#   
###############################################################################

def FastBzgrep(files, pattern, ignore_case):

    buffPonies = max(1, os.cpu_count() - 1)
    validFiles = [file for file in files if os.path.isfile(file)]

    if not validFiles:
        print("[INFO] No bz2 files to process.")
        return

    with ProcessPoolExecutor(max_workers=buffPonies) as executor:
        results = executor.map(SearchInBz2, validFiles, [pattern] * len(validFiles), [ignore_case] * len(validFiles))

    for matchList in results:
        if matchList:
            sys.stdout.write("\n".join(matchList) + "\n")


###############################################################################
#
# Procedure   : FastGzgrep()
#
# Description : Searches for regex pattern inside compressed .gz files.
#             : - Uses aggressive parallel processing.
#             : - Uses ProcessPoolExecutor for true multi-core execution.
#     
# Input       : files   - List of .gz files to search.
#             : pattern - Regex string to match inside files.
#             
# Returns     : -none-    
#     
###############################################################################

def FastGzgrep(files, pattern, ignore_case):

    buffPonies = max(1, os.cpu_count() - 1)
    validFiles = [file for file in files if os.path.isfile(file)]

    if not validFiles:
        print("[INFO] No gz files to process.")
        return

    with ProcessPoolExecutor(max_workers=buffPonies) as executor:
        results = executor.map(SearchInGz, validFiles, [pattern] * len(validFiles), [ignore_case] * len(validFiles))

    for matchList in results:
        if matchList:
            sys.stdout.write("\n".join(matchList) + "\n")


###############################################################################
#             
# Procedure   : Main()
#
# Description : Entry point.
#     
# Input       : -none-    
#             
# Returns     : -none-    
#     
###############################################################################

def Main():

    args = ParseArgs() 

    if args.help or not args.pattern or not args.files:
        DisplayHelp()
        return

    files        = args.files
    pattern      = args.pattern
    countOnly    = args.count
    recursive    = args.recursive
    buffPonies   = args.workers
    ignoreCase   = args.ignore_case
    outputFile   = args.output

    matchedLines = []

    #
    # handles recursive search 
    #

    allFiles = []

    for path in files:

        if os.path.isdir(path) and recursive:
            for root, _, filenames in os.walk(path):
                for name in filenames:
                    allFiles.append(os.path.join(root, name))

        elif os.path.isfile(path):
            allFiles.append(path)

        else:
            print(f"[WARNING] Skipping invalid path: {path}")

    #
    # clasify by file type 
    #

    bzFiles   = []
    gzFiles   = []
    flatFiles = []

    for file in allFiles:

        fileType = DetectFileType(file)

        if fileType == 'bz':
            bzFiles.append(file)

        elif fileType == 'gz':
            gzFiles.append(file)

        else:
            flatFiles.append(file)

    if bzFiles:
        matchedLines += SearchFilesInParallel(SearchInBz2, bzFiles, pattern, ignoreCase, countOnly, buffPonies)

    if gzFiles:
        matchedLines += SearchFilesInParallel(SearchInGz, gzFiles, pattern, ignoreCase, countOnly, buffPonies)

    if flatFiles:
        matchedLines += SearchFilesInParallel(SearchInFlat, flatFiles, pattern, ignoreCase, countOnly, buffPonies)

    if outputFile:

        try:
            with open(outputFile, 'w') as file:
                file.write("\n".join(matchedLines) + "\n")

        except Exception as e:
            print(f"[ERROR] Could not write to output file: {e}")

    else:
        if matchedLines:
            print("\n".join(matchedLines))


if __name__ == "__main__":
    Main()

