mod std_writer;

use clap::Parser;
use sqlx::mysql::{MySqlColumn, MySqlPoolOptions, MySqlRow};
use sqlx::types::chrono::Local;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row};
use std::fmt::Display;
use std_writer::StdWriter;
use url::Url;

/// Standalone database dump tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help(true))]
struct Args {
    /// Schema to extract
    #[arg(short, long, required = false)]
    schema: Option<String>,

    /// Database url to connect to
    #[arg(short, long)]
    url: String,

    /// Extract schema only
    #[arg(short = 'd', long = "no-data", default_value_t = false)]
    exclude_data: bool,

    /// Filename to write output to
    #[arg(short, long = "output-file", required = false)]
    output_file: Option<String>,

    /// Rename the schema
    #[arg(long = "new-schema-name", required = false)]
    renamed_schema_name: Option<String>,

    /// Don't create the schema
    #[arg(long = "no-create-schema", required = false, default_value_t = true)]
    create_schema: bool,

    /// Use single row inserts
    #[arg(long = "single-row-inserts", required = false, default_value_t = false)]
    single_row_inserts: bool,

    /// BETA: Skip any datatype we don't understand - set the field to null
    #[arg(
        long = "beta-skip-unknown-datatypes",
        required = false,
        default_value_t = false
    )]
    skip_unknown_datatypes: bool,
}

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    //
    // Parse the command line arguments and set the writer to write to a file or STDOUT
    //
    let args = Args::parse();
    let mut w = StdWriter::new(args.output_file);
    let max_insert_count = if args.single_row_inserts { 1 } else { 100 };
    let schema = if let Some(schema) = args.schema {
        schema
    } else {
        let url = Url::parse(&args.url);
        let url = url.expect("Invalid url, unable to parse");
        url.path()
            .split_once('/')
            .expect("Unable to obtain the schema. Either include the schema name as part of the url or pass it using the --schema argument")
            .1
            .to_string()
    };

    //
    // Create a pool of connections, probably not required as we currently only use one connection
    //
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&args.url)
        .await?;

    write_header(&mut w, &schema, &args.url);

    //
    // Grab all of the tables from the selected schema
    let rows: Vec<(String,)> =
        sqlx::query_as("select table_name from information_schema.tables where table_schema=?")
            .bind(&schema)
            .fetch_all(&pool)
            .await?;

    if let Some(schema) = args.renamed_schema_name {
        if args.create_schema {
            w.println(format!("create schema {};", &schema).as_str());
        }
        w.println(format!("use {};", &schema).as_str());
    } else {
        if args.create_schema {
            w.println(format!("create schema {};", &schema).as_str());
        }
        w.println(format!("use {};", &schema).as_str());
    }
    w.println("SET FOREIGN_KEY_CHECKS=0;");
    for row in &rows {
        w.println(format!("-- Extract DDL for table {}", row.0).as_str());
        let ddl: (String, String) =
            sqlx::query_as(&format!("SHOW CREATE TABLE {}.{}", &schema, &row.0))
                .fetch_one(&pool)
                .await?;
        w.println(format!("{};", ddl.1).as_str());
    }
    if !args.exclude_data {
        for row in &rows {
            w.println(format!("-- Extracting data for {}", row.0).as_str());
            let mut count = 0;
            // query table
            let data_rows = sqlx::query::<_>(&format!("select * from {}.{}", &schema, &row.0))
                .fetch_all(&pool)
                .await?;
            if data_rows.len() == 0 {
                continue;
            }
            let column_names = compute_column_name(data_rows.get(0).unwrap().columns());
            for i in 0..data_rows.len() {
                let data = data_rows.get(i);
                if data.is_none() {
                    continue;
                }
                let data = data.unwrap();
                if data.is_empty() {
                    continue;
                }
                if count == 0 {
                    w.print(format!("insert into {} ({}) values(", row.0, column_names).as_str());
                }

                let cols = data.columns().len();
                for i in 0..cols - 1 {
                    let value = cast_data(&data, i, args.skip_unknown_datatypes);
                    if let Some(value) = value {
                        w.print(format!("{},", value).as_str());
                    } else {
                        w.print("NULL,");
                    }
                }

                let value = cast_data(&data, cols - 1, args.skip_unknown_datatypes);
                if let Some(value) = value {
                    w.print(format!("{}", value).as_str());
                } else {
                    w.print("NULL");
                }

                count = count + 1;
                if count % max_insert_count == 0 {
                    w.print(
                        format!("));\ninsert into {} ({}) values(", row.0, column_names).as_str(),
                    );
                } else {
                    if i >= data_rows.len() - 1 {
                        w.println(");");
                    } else {
                        w.print("),\n\t(");
                    }
                }
            }
        }
    }
    w.println("SET FOREIGN_KEY_CHECKS=1;");
    w.flush();
    Ok(())
}

pub fn cast_data(row: &MySqlRow, index: usize, skip_unknown_datatypes: bool) -> Option<String> {
    let col = row.column(index);
    let type_name = col.type_info().to_string();

    match type_name.as_str() {
        "BOOLEAN" => to_string(row.try_get::<bool, usize>(index), false),
        "TINYINT" => to_string(row.try_get::<i8, usize>(index), false),
        "SMALLINT" => to_string(row.try_get::<i16, usize>(index), false),
        "INT" => to_string(row.try_get::<i32, usize>(index), false),
        "BIGINT" => to_string(row.try_get::<i64, usize>(index), false),
        "TINYINT UNSIGNED" => to_string(row.try_get::<u8, usize>(index), false),
        "SMALLINT UNSIGNED" => to_string(row.try_get::<u16, usize>(index), false),
        "INT UNSIGNED" => to_string(row.try_get::<u32, usize>(index), false),
        "BIGINT UNSIGNED" => to_string(row.try_get::<u64, usize>(index), false),
        "FLOAT" => to_string(row.try_get::<f32, usize>(index), false),
        "DOUBLE" => to_string(row.try_get::<f64, usize>(index), false),
        "CHAR" => to_string(row.try_get::<String, usize>(index), true),
        "VARCHAR" => to_string(row.try_get::<String, usize>(index), true),
        "TEXT" => to_string(row.try_get::<String, usize>(index), true),
        "TIMESTAMP" => to_date_string(row.try_get::<DateTime<Utc>, usize>(index)),
        "DATETIME" => to_date_string(row.try_get::<NaiveDateTime, usize>(index)),
        "DATE" => to_date_string(row.try_get::<NaiveDate, usize>(index)),
        "TIME" => to_date_string(row.try_get::<NaiveTime, usize>(index)),
        "DECIMAL" => to_string(row.try_get::<BigDecimal, usize>(index), false),
        "ENUM" => to_string(row.try_get::<String, usize>(index), true),
        // "AddOtherTypesHere" => to_string(row.try_get::<i64, usize>(index), false),
        // Add support for Binary data
        "VARBINARY" => None,
        "BINARY" => None,
        "BLOB" => None,

        _ => {
            if skip_unknown_datatypes {
                None
            } else {
                panic!("The database type {} is not implemented in this version of dbdump. Please try to download a more recent version or report a bug if you are on the most recent version", type_name)
            }
        }
    }
}

fn quote(str: String) -> String {
    format!("'{}'", str)
}

fn compute_column_name(columns: &[MySqlColumn]) -> String {
    columns
        .into_iter()
        .map(|x| x.name().to_string())
        .collect::<Vec<String>>()
        .join(",")
}

fn write_header(writer: &mut StdWriter, schema: &String, url: &String) {
    writer.println("-- -----------------------------------------------------------------------------------------");
    writer.println("-- Database Dump Tool v0.2.0");
    writer.println("-- https://github.com/wcherry/dbdump");
    writer.println("-- ");
    writer.println(format!("-- Created at {}", Local::now()).as_str());
    writer.println(format!("-- Schema: {}", schema).as_str());
    writer.println(format!("-- URL: {}", url).as_str());
    writer.println("-- -----------------------------------------------------------------------------------------");
}

fn to_string<T: Display>(n: Result<T, sqlx::Error>, q: bool) -> Option<String> {
    if let Ok(v) = n {
        Some(if q {
            quote(v.to_string())
        } else {
            v.to_string()
        })
    } else {
        None
    }
}

fn to_date_string<T: Display>(n: Result<T, sqlx::Error>) -> Option<String> {
    if let Ok(v) = n {
        // Strip off the UTC that is added to Timestamps
        let str = if v.to_string().ends_with("UTC") {
            let s = v.to_string();
            s[0..s.len() - 4].to_string()
        } else {
            v.to_string()
        };
        Some(format!("'{}'", str))
    } else {
        None
    }
}
