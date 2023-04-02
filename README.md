# dbdump

## A simple database dump tool written in Rust to be super fast.

## Installation

Currently builds are produced OSX only for both the Intel and Apple chip-set.

- Download the latest [release](https://github.com/wcherry/dbdump/releases)
- Unzip the downloaded zip file - it will create a directory for each supported operating system
- Select the correct operating system and platform (e.g. macos/intel)
- Copy the executable from the correct folder and copy to where you want to execute it from (e.g. `~/bin`)
- Run the executable supplying no-arguments - It will typically fail the first time you run it due to security constraints
  - Grant access to dbdump in your security settings
  - Run a second time - it will display the usage instructions

The are no other required resources to run.

Check the [build](#building-from-source) section for instructions on building the executable for other platforms.

## Uninstall

Delete the executable.

## Example Usages

Getting Help

```
❯ dbdump
Standalone database dump tool

Usage: dbdump [OPTIONS] --url <URL>

Options:
  -s, --schema <SCHEMA>
          Schema to extract
  -u, --url <URL>
          Database url to connect to
  -d, --no-data
          Extract schema only
  -o, --output-file <OUTPUT_FILE>
          Filename to write output to
      --new-schema-name <RENAMED_SCHEMA_NAME>
          Rename the schema
      --no-create-schema
          Don't create the schema
      --single-row-inserts
          Use single row inserts
      --beta-skip-unknown-datatypes
          BETA: Skip any datatype we don't understand - set the field to null
  -h, --help
          Print help
  -V, --version
          Print version
```

Simple export

```
> dbdump  -u "mysql://USER_NAME:PASSWORD@localhost/test -o test.sql
```

## Known Limitations

- Check the [issues](https://github.com/wcherry/dbdump/issues) page for a list of future features and any bugs reported.
- Currently only MariaDB/MySQL are supported - [more](https://github.com/wcherry/dbdump/issues/4) databases are planned to be supported in the near future.

## Building from Source

- Install the latest version of Rust using `rustup`
- Clone the source code to you machine
- Run `cargo build` from inside your project directory (e.g. `~/projects/dbdump`)
