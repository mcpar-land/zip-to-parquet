# `zip-to-parquet`

A really simple command line utility. Takes a `.zip` file and turns it into a parquet file with two columns:

- `name`: the full name and path of the file. (string)
- `body`: the body of the file as a binary. (binary)

Uses 1024MB blocks, and Snappy compression.

## Options

### `-i, --input <INPUT>`

Provide a path to a zip file to convert. Can be specified multiple times to include multiple zip files. Can also include globs.

### `-o, --output <OUTPUT>`

Specify the location of the output parquet file. Can only specify one.

### `--stdout`

Output to stdout instead of to a file.

### `--no-body`

Exclude the body of all the files. The `body` column will still be present in the parquet file, but every value will be null. This significantly speeds up creation and reduces the resulting file size. Useful if you only need file names and paths.

### `--glob <GLOB>`

Provide a glob that filters out files in the zip. Uses the [wax](https://github.com/olson-sean-k/wax) crate, refer to their documentation for syntax.

Note that most zip files don't keep their contents in the root level directory, so a simply glob like `*.png` won't pick up anything in files inside of folders. You'll almost always want to use `**/*.png` or similar for your glob.

### `-h, --help`

Prints the help screen.

### `-v, --version`

Prints the version.

---

Example usage:

```
zip-to-parquet -i ~/downloads/my_cool_zip.zip -i ~/downloads/my_other_cool_zip.zip -o ~/my_new_parquet.parquet
```

This is a utility for some domain-specific data parsing involving very high numbers of files that are initially stored in zips. It's faster to incorporate them into data pipelines by converting them to parquet files, instead of unzipping to disc.
