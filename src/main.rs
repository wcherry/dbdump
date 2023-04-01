mod std_writer;

use chrono;
use clap::Parser;
use sqlx::mysql::{MySqlColumn, MySqlPoolOptions, MySqlRow};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row};
use std_writer::StdWriter;

/// Standalone database dump tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Schema to extract
    #[arg(short, long, default_value_t = String::from("test"))]
    schema: String,

    /// Database url to connect to
    #[arg(short, long, env = "DATABASE_URL")]
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
                    let value = cast_data(&data, i);
                    if let Some(value) = value {
                        w.print(format!("{},", value).as_str());
                    } else {
                        w.print("NULL,");
                    }
                }

                let value = cast_data(&data, cols - 1);
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
    w.println("SET FOREIGN_KEY_CHECKS=0;");
    w.flush();
    Ok(())
}

fn cast_data(row: &MySqlRow, index: usize) -> Option<String> {
    let result = row.try_get::<i64, usize>(index);
    if result.is_ok() {
        return if let Some(value) = result.ok() {
            Option::Some(value.to_string())
        } else {
            None
        };
    }

    let result = row.try_get::<i32, usize>(index);
    if result.is_ok() {
        return if let Some(value) = result.ok() {
            Option::Some(value.to_string())
        } else {
            None
        };
    }

    let result = row.try_get::<BigDecimal, usize>(index);
    if result.is_ok() {
        return if let Some(value) = result.ok() {
            Option::Some(value.to_string())
        } else {
            None
        };
    }

    let result = row.try_get::<String, usize>(index);
    if result.is_ok() {
        return if let Some(value) = result.ok() {
            Option::Some(format!("'{}'", value))
        } else {
            None
        };
    }

    let result = row.try_get::<bool, usize>(index);
    if result.is_ok() {
        return if let Some(value) = result.ok() {
            Option::Some(value.to_string())
        } else {
            None
        };
    }

    println!(
        "\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!! Failed to parse data: {:?} !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n",
        row.try_get::<String, usize>(index).err()
    );

    None
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
    writer.println(format!("-- Created at {}", chrono::offset::Local::now()).as_str());
    writer.println(format!("-- Schema: {}", schema).as_str());
    writer.println(format!("-- URL: {}", url).as_str());
    writer.println("-- -----------------------------------------------------------------------------------------");
}
