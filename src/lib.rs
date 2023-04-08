pub mod std_writer;

use sqlx::mysql::{MySql, MySqlColumn, MySqlRow};
use sqlx::pool::Pool;
use sqlx::types::chrono::Local;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row};
use std::fmt::Display;
use std_writer::StdWriter;

pub async fn export_tables(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    //
    // Grab all of the tables from the selected schema
    let table_names: Vec<(String,)> =
        sqlx::query_as("select table_name from information_schema.tables where table_schema=? and table_type='BASE TABLE'")
            .bind(&schema)
            .fetch_all(pool)
            .await?;

    for row in &table_names {
        writer.println(format!("-- Extract DDL for table {}", row.0).as_str());
        let ddl: (String, String) =
            sqlx::query_as(&format!("SHOW CREATE TABLE {}.{}", &schema, &row.0))
                .fetch_one(pool)
                .await?;
        writer.println(format!("{};", ddl.1).as_str());
    }
    Ok(())
}

pub async fn export_views(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract views
    let views: Vec<(String, String)> = sqlx::query_as(
        "select table_name, view_definition from information_schema.views where table_schema=?",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for row in &views {
        writer.println(format!("-- Extract DDL for view {}", row.0).as_str());
        writer.println(format!("create view {} as {};", row.0, row.1).as_str());
    }
    Ok(())
}

pub async fn export_stored_procs(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract stored procedures - only support body type of SQL
    let routines: Vec<(String,String)> = sqlx::query_as(
        "select routine_name, routine_definition from information_schema.routines where routine_schema=? and routine_body='SQL' and routine_type='PROCEDURE'",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for row in &routines {
        // get the parameters
        let parameters: Vec<(String,String,String)> = sqlx::query_as(
        "select parameter_mode, parameter_name, dtd_identifier from information_schema.parameters where specific_schema=? and specific_name=? and routine_type='PROCEDURE'",
        )
        .bind(&schema)
        .bind(&row.0)
        .fetch_all(pool)
        .await?;

        //join params into p.0 p.1 p.2
        let p_str = parameters
            .into_iter()
            .map(|p| format!("{} {} {}", p.0, p.1, p.2))
            .collect::<Vec<String>>()
            .join(",");

        writer.println(format!("-- Extract DDL for view {}", row.0).as_str());
        writer.println(format!("create procedure {}({})\n{};", row.0, p_str, row.1).as_str());
    }
    Ok(())
}

pub async fn export_functions(
    _pool: &Pool<MySql>,
    _writer: &mut StdWriter,
    _schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract functions - only support body type of SQL
    // let routines: Vec<(String,String,String)> = sqlx::query_as(
    //     "select routine_name, routine_definition from information_schema.tables where table_schema=? and routine_body='SQL' and routine_type='FUNCTION'",
    // )
    // .bind(&schema)
    // .fetch_all(pool)
    // .await?;
    // for row in &routines {
    //     w.println(format!("-- Extract DDL for view {}", row.0).as_str());
    //     w.println(format!("create function {}\n {};", row.0, row.1).as_str());
    // }

    Ok(())
}

pub async fn export_data(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
    single_row_inserts: bool,
    skip_unknown_datatypes: bool,
) -> Result<(), sqlx::Error> {
    let max_insert_count = if single_row_inserts { 1 } else { 100 };

    // Grab all of the tables from the selected schema
    let table_names: Vec<(String,)> =
        sqlx::query_as("select table_name from information_schema.tables where table_schema=? and table_type='BASE TABLE'")
            .bind(&schema)
            .fetch_all(pool)
            .await?;

    for row in &table_names {
        writer.println(format!("-- Extracting data for {}", row.0).as_str());
        let mut count = 0;
        // query table
        let data_rows = sqlx::query::<_>(&format!("select * from {}.{}", &schema, &row.0))
            .fetch_all(pool)
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
                writer.print(format!("insert into {} ({}) values(", row.0, column_names).as_str());
            }

            let cols = data.columns().len();
            for i in 0..cols - 1 {
                let value = cast_data(&data, i, skip_unknown_datatypes);
                if let Some(value) = value {
                    writer.print(format!("{},", value).as_str());
                } else {
                    writer.print("NULL,");
                }
            }

            let value = cast_data(&data, cols - 1, skip_unknown_datatypes);
            if let Some(value) = value {
                writer.print(format!("{}", value).as_str());
            } else {
                writer.print("NULL");
            }

            count = count + 1;
            if count % max_insert_count == 0 {
                writer.print(
                    format!(");\ninsert into {} ({}) values(", row.0, column_names).as_str(),
                );
            } else {
                if i >= data_rows.len() - 1 {
                    writer.println(");");
                } else {
                    writer.print("),\n\t(");
                }
            }
        }
    }

    Ok(())
}

pub fn cast_data(row: &MySqlRow, index: usize, skip_unknown_datatypes: bool) -> Option<String> {
    let col = row.column(index);
    let type_name = col.type_info().to_string();

    match type_name.as_str() {
        "BOOLEAN" => to_string(row.try_get::<bool, usize>(index), false),
        "TINYINT" => to_string(row.try_get::<i8, usize>(index), false),
        "BIT" => to_string(row.try_get::<bool, usize>(index), false),
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
    format!("\"{}\"", str)
}

fn compute_column_name(columns: &[MySqlColumn]) -> String {
    columns
        .into_iter()
        .map(|x| x.name().to_string())
        .collect::<Vec<String>>()
        .join(",")
}

pub fn write_header(writer: &mut StdWriter, schema: &String, url: &String) {
    writer.println("-- -----------------------------------------------------------------------------------------");
    writer.println("-- Database Dump Tool v0.3.1");
    writer.println("-- https://github.com/wcherry/dbdump");
    writer.println("-- ");
    writer.println(format!("-- Created at {}", Local::now()).as_str());
    writer.println(format!("-- Schema: {}", schema).as_str());
    writer.println(format!("-- URL: {}", url).as_str());
    writer.println("-- -----------------------------------------------------------------------------------------");
}

pub fn write_prefix(
    writer: &mut StdWriter,
    source_schema: &String,
    target_schema: Option<String>,
    create_schema: bool,
    disable_check: bool,
) {
    let schema = target_schema.unwrap_or(source_schema.clone());

    if create_schema {
        writer.println(format!("create schema if not EXISTS {};", &schema).as_str());
    }
    writer.println(format!("use {};", &schema).as_str());
    if disable_check {
        writer.println("SET FOREIGN_KEY_CHECKS=0;");
    }
}

pub fn write_postfix(writer: &mut StdWriter, disable_check: bool) {
    if disable_check {
        writer.println("SET FOREIGN_KEY_CHECKS=0;");
    }
}

pub fn write_footer(writer: &mut StdWriter) {
    writer.flush();
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