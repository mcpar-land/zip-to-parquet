# `zip-to-parquet`

A really simple command line utility. Takes a `.zip` file and turns it into a parquet file with two columns:

- `name`: the full name and path of the file. (string)
- `body`: the body of the file as a binary. (binary)

Uses 1024MB blocks, and Snappy compression.

Takes two command line arguments:

- path to the `.zip` file to read
- path to the `.parquet` file to write to / create

Example usage:

```
zip-to-parquet ~/downloads/my_cool_zip.zip ~/my_new_parquet.parquet
```
