#!/bin/bash

# Path to the license header file
LICENSE_HEADER="LICENSE_HEADER.txt"

# Temporary file
TEMP_FILE="temp_file.tmp"

# Find and prepend the license header to each Python file
find . -type f -name '*.py' | while read file; do
    cat $LICENSE_HEADER $file > $TEMP_FILE && mv $TEMP_FILE $file
done

# Clean up if temp file accidentally left behind
rm -f $TEMP_FILE

