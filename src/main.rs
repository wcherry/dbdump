mod std_writer;

use clap::Parser;

use sqlx::mysql::{MySqlColumn, MySqlPoolOptions, MySqlRow};
use sqlx::types::chrono::Local;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row};
use std_writer::StdWriter;

/// Standalone database dump tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help(true))]
struct Args {
    /// Schema to extract
    #[arg(short, long, default_value_t = String::from("test"))]
    schema: String,

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

    //
    // Create a pool of connections, probably not required as we currently only use one connection
    //
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&args.url)
        .await?;

    write_header(&mut w, &args.schema, &args.url);

    //
    // Grab all of the tables from the selected schema
    let rows: Vec<(String,)> =
        sqlx::query_as("select table_name from information_schema.tables where table_schema=?")
            .bind(&args.schema)
            .fetch_all(&pool)
            .await?;
    if let Some(schema) = args.renamed_schema_name {
        w.println(format!("use {};", schema).as_str());
    } else {
        w.println(format!("use {};", &args.schema).as_str());
    }
    w.println("SET FOREIGN_KEY_CHECKS=0;");
    for row in &rows {
        w.println(format!("-- Extract DDL for table {}", row.0).as_str());
        let ddl: (String, String) =
            sqlx::query_as(&format!("SHOW CREATE TABLE {}.{}", &args.schema, &row.0))
                .fetch_one(&pool)
                .await?;
        w.println(format!("{};", ddl.1).as_str());
    }
    if !args.exclude_data {
        for row in &rows {
            w.println(format!("-- Extracting data for {}", row.0).as_str());
            let mut count = 0;
            // query table
            let data_rows = sqlx::query::<_>(&format!("select * from {}.{}", &args.schema, &row.0))
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

    /*
    This check protects against null data - lots of trail and error to get to this particular code that actually works
    */
    if row.try_get_unchecked::<&str, usize>(index).ok().is_none()
        && row.try_get_unchecked::<i64, usize>(index).ok().is_none()
    {
        return None;
    }

    match type_name.as_str() {
        "BOOLEAN" => Some(row.get::<bool, usize>(index).to_string()),
        "TINYINT" => Some(row.get::<i8, usize>(index).to_string()),
        "SMALLINT" => Some(row.get::<i16, usize>(index).to_string()),
        "INT" => Some(row.get::<i32, usize>(index).to_string()),
        "BIGINT" => Some(row.get::<i64, usize>(index).to_string()),
        "TINYINT UNSIGNED" => Some(row.get::<u8, usize>(index).to_string()),
        "SMALLINT UNSIGNED" => Some(row.get::<u16, usize>(index).to_string()),
        "INT UNSIGNED" => Some(row.get::<u32, usize>(index).to_string()),
        "BIGINT UNSIGNED" => Some(row.get::<u64, usize>(index).to_string()),
        "FLOAT" => Some(row.get::<f32, usize>(index).to_string()),
        "DOUBLE" => Some(row.get::<f64, usize>(index).to_string()),
        "CHAR" => Some(quote(row.get::<String, usize>(index))),
        "VARCHAR" => Some(quote(row.get::<String, usize>(index))),
        "TEXT" => Some(quote(row.get::<String, usize>(index))),
        "TIMESTAMP" => Some(quote(row.get::<DateTime<Utc>, usize>(index).to_string())),
        "DATETIME" => Some(quote(row.get::<NaiveDateTime, usize>(index).to_string())),
        "DATE" => Some(quote(row.get::<NaiveDate, usize>(index).to_string())),
        "TIME" => Some(quote(row.get::<NaiveTime, usize>(index).to_string())),
        "DECIMAL" => Some(row.get::<BigDecimal, usize>(index).to_string()),
        // "AddOtherTypesHere" => Some(row.get::<i64, usize>(index).to_string()),
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
