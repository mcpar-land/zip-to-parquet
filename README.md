# `zip-to-parquet`

A really simple command line utility. Takes a `.zip` file and turns it into a parquet file with two columns:

- `name`: the full name and path of the file. (string)
- `body`: the body of the file as a binary. (binary)

Uses 1024MB blocks, and Snappy compression.

```
Convert .zip file to parquet of all files inside

Usage: zip-to-parquet [OPTIONS]

Options:
  -i, --input <INPUT>    .zip file input path
  -o, --output <OUTPUT>  .parquet file output path
      --stdout           use stdout for output
      --no-body          Do not load or include file bodies in output (significantly reduce size and time!)
  -h, --help             Print help
  -V, --version          Print version
```

Example usage:

```
zip-to-parquet -i ~/downloads/my_cool_zip.zip -i ~/downloads/my_other_cool_zip.zip -o ~/my_new_parquet.parquet
```

This is a utility for some domain-specific data parsing involving very high numbers of files.
