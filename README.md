# `zip-to-parquet`

A really simple command line utility. Takes a `.zip` file and turns it into a `.parquet` file with following columns:

| Column Name | Column Type | Description                               |
| ----------- | ----------- | ----------------------------------------- |
| `name`      | `varchar`   | The full name of the file                 |
| `source`    | `varchar`   | The path to the original zip file         |
| `body`      | `blob`      | A binary blob of the contents of the file |

Uses 1024MB blocks, and Snappy compression.

This is a utility for some domain-specific data parsing involving very high numbers of files that are initially stored in zips. It's faster to incorporate them into data pipelines by converting them to parquet files, instead of unzipping to disc.

## Examples

Get help on all options:

```sh
  zip-to-parquet --help
```
---

Convert a zip to a parquet:

```sh
zip-to-parquet -i ~/downloads/my_cool_zip.zip -i ~/downloads/my_other_cool_zip.zip -o ~/my_new_parquet.parquet
```
---

Convert all zips in `/data/lots_of_zips/` and `/data/other_zips/` to a parquet, only including `.png` files:

```sh
  zip-to-parquet -i "/data/lots_of_zips/**/*.zip" -i "/data/other_zips/**/*.zip" -o ~/my_new_parquet.parquet -g "**/*.png"
```

Be careful with globs as arguments, as some shells will automatically expand paths with asterixes in them if not wrapped in quotes.

---

Put only the names of files in a zip file into a parquet:

```sh
  zip-to-parquet -i my_cool_zip.zip -o my_new_parquet.parquet --no-body --no-source
```
