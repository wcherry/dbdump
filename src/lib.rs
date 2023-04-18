pub mod logger;
pub mod std_writer;
use logger::Logger;
use regex::Regex;
use sqlx::mysql::{MySql, MySqlColumn, MySqlRow};
use sqlx::pool::Pool;
use sqlx::types::chrono::Local;
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row};
use std::fmt::Display;
use std_writer::StdWriter;

//
// Export the table DDL - tables are ordered so that we try and
//   avoid any table dependencies.
//
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

    let table_names = order_tables(pool, schema, table_names).await?;

    for table_name in &table_names {
        writer.println(format!("-- Extract DDL for table {}", table_name).as_str());
        let ddl: (String, String) =
            sqlx::query_as(&format!("SHOW CREATE TABLE {}.{}", &schema, &table_name))
                .fetch_one(pool)
                .await?;
        writer.println(format!("{};", ddl.1).as_str());
    }
    Ok(())
}

//
// Export the view DDL - views are ordered so that we try and
//   avoid any view dependencies.
//
pub async fn export_views(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract views
    let view_names: Vec<(String,)> =
        sqlx::query_as("select table_name from information_schema.tables where table_schema=? and table_type='VIEW'")
            .bind(&schema)
            .fetch_all(pool)
            .await?;

    let view_names = order_views(pool, schema, view_names).await?;

    for name in &view_names {
        writer.println(format!("-- Extract DDL for view {}", name).as_str());
        let ddl: (String, String) =
            sqlx::query_as(&format!("SHOW CREATE VIEW {}.{}", &schema, name))
                .fetch_one(pool)
                .await?;
        writer.println(format!("{};", ddl.1).as_str());
    }
    Ok(())
}

pub async fn export_stored_procs(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract stored procedures - only support body type of SQL
    let routines: Vec<(String,)> = sqlx::query_as(
        "select routine_name from information_schema.routines where routine_schema=? and routine_body='SQL' and routine_type='PROCEDURE'",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for row in &routines {
        // get the parameters
        let (procedure, sql_mode, ddl, character_set, collation, db_collation): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as(format!("show create procedure {}.{}", &schema, &row.0).as_str())
            .fetch_one(pool)
            .await?;

        writer.println(format!("-- Extract DDL for stored procedure {}", procedure).as_str());
        writer.println(format!("-- SQL Mode {}", sql_mode).as_str());
        writer.println(format!("-- Character Set {}", character_set).as_str());
        writer.println(format!("-- Collation {}", collation).as_str());
        writer.println(format!("-- Database Collation {}", db_collation).as_str());

        writer.println("DELIMITER ;;");
        writer.println(format!("{};;", ddl).as_str());
        writer.println("DELIMITER ;");
    }
    Ok(())
}

pub async fn export_functions(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract stored procedures - only support body type of SQL
    let routines: Vec<(String,)> = sqlx::query_as(
        "select routine_name from information_schema.routines where routine_schema=? and routine_body='SQL' and routine_type='FUNCTION'",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for row in &routines {
        // get the parameters
        let (procedure, sql_mode, ddl, character_set, collation, db_collation): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as(format!("show create function {}.{}", &schema, &row.0).as_str())
            .fetch_one(pool)
            .await?;

        writer.println(format!("-- Extract DDL for function {}", procedure).as_str());
        writer.println(format!("-- SQL Mode {}", sql_mode).as_str());
        writer.println(format!("-- Character Set {}", character_set).as_str());
        writer.println(format!("-- Collation {}", collation).as_str());
        writer.println(format!("-- Database Collation {}", db_collation).as_str());

        writer.println("DELIMITER ;;");
        writer.println(format!("{};;", ddl).as_str());
        writer.println("DELIMITER ;");
    }
    Ok(())
}

pub async fn export_triggers(
    pool: &Pool<MySql>,
    writer: &mut StdWriter,
    schema: &String,
) -> Result<(), sqlx::Error> {
    // Extract stored procedures - only support body type of SQL
    let triggers: Vec<(String,)> = sqlx::query_as(
        "select trigger_name from information_schema.triggers where trigger_schema=?",
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    for row in &triggers {
        // get the parameters
        let (trigger, sql_mode, ddl, character_set, collation, db_collation): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as(format!("show create trigger {}.{}", &schema, &row.0).as_str())
            .fetch_one(pool)
            .await?;

        writer.println(format!("-- Extract DDL for trigger {}", trigger).as_str());
        writer.println(format!("-- SQL Mode {}", sql_mode).as_str());
        writer.println(format!("-- Character Set {}", character_set).as_str());
        writer.println(format!("-- Collation {}", collation).as_str());
        writer.println(format!("-- Database Collation {}", db_collation).as_str());

        writer.println("DELIMITER ;;");
        writer.println(format!("{};;", ddl).as_str());
        writer.println("DELIMITER ;");
    }
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

    'tables: for row in &table_names {
        writer.println(format!("-- Extracting data for {}", row.0).as_str());
        let mut count = 0;
        // query table
        let data_rows = sqlx::query::<_>(&format!("select * from {}.{}", &schema, &row.0))
            .fetch_all(pool)
            .await?;
        if data_rows.len() == 0 {
            continue 'tables;
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
            if count % max_insert_count == 0 {
                writer
                    .print(format!("insert into `{}` ({}) values(", row.0, column_names).as_str());
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
                writer.print(");\n");
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
    format!(
        "'{}'",
        str.replace("'", "''")
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
    )
}

fn compute_column_name(columns: &[MySqlColumn]) -> String {
    columns
        .into_iter()
        .map(|x| format!("`{}`", x.name()))
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
        writer.println("SET FOREIGN_KEY_CHECKS=1;");
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

async fn order_tables(
    pool: &Pool<MySql>,
    schema: &String,
    tables: Vec<(String,)>,
) -> Result<Vec<String>, sqlx::Error> {
    let mut sorted_tables: Vec<String> = tables.iter().map(|t| t.0.to_string()).collect();

    let rows : Vec<(String, String)>= sqlx::query_as("select TABLE_NAME,REFERENCED_TABLE_NAME from information_schema.REFERENTIAL_CONSTRAINTS where CONSTRAINT_SCHEMA=?")
    .bind(&schema)
    .fetch_all(pool)
    .await?;

    for row in rows {
        //let mut it = sorted_tables.iter();
        let tab_index = sorted_tables
            .iter()
            .position(|s| s.eq_ignore_ascii_case(&row.0));
        let ref_index = sorted_tables
            .iter()
            .position(|s| s.eq_ignore_ascii_case(&row.1));
        if tab_index.is_none() {
            Logger::info(format!(
                "Found a reference to a table {} that doesn't exists",
                row.0
            ));
            eprintln!("{}", sorted_tables.join(","));
            continue;
        }
        let tab_index = tab_index.unwrap();
        if ref_index.is_none() {
            Logger::info(format!(
                "Found a referenced table {} that doesn't exists for {}",
                row.1, row.0
            ));
            continue;
        }
        let ref_index = ref_index.unwrap();

        if ref_index > tab_index {
            let el = sorted_tables.remove(ref_index);
            sorted_tables.insert(tab_index, el);
        }
    }
    return Ok(sorted_tables);
}

async fn order_views(
    pool: &Pool<MySql>,
    schema: &String,
    views: Vec<(String,)>,
) -> Result<Vec<String>, sqlx::Error> {
    let from_regex = Regex::new(r"from\s+(\()?`[^`]+`\.`([^`]+)`").unwrap();
    let join_regex = Regex::new(r"join\s+(\()?`[^`]+`\.`([^`]+)`").unwrap();

    let mut sorted_views = views.iter().map(|t| t.0.to_string()).collect();
    for view in views {
        let ddl: (String, String) =
            sqlx::query_as(&format!("SHOW CREATE VIEW {}.{}", &schema, &view.0))
                .fetch_one(pool)
                .await?;
        for grp in from_regex.captures_iter(&ddl.1) {
            sorted_views = reorder_vec(sorted_views, &view.0, &grp[0].to_string());
        }
        for grp in join_regex.captures_iter(&ddl.1) {
            sorted_views = reorder_vec(sorted_views, &view.0, &grp[0].to_string());
        }
    }

    return Ok(sorted_views);
}

fn reorder_vec(mut vec: Vec<String>, table_name: &String, ref_name: &String) -> Vec<String> {
    let mut it = vec.iter();
    let tab_index = it.position(|s| s.eq_ignore_ascii_case(table_name));
    let ref_index = it.position(|s| s.eq_ignore_ascii_case(ref_name));
    if tab_index.is_none() {
        Logger::info(format!(
            "Found a reference to a table/view {table_name} that doesn't exists"
        ));
        return vec;
    }
    let tab_index = tab_index.unwrap();
    if ref_index.is_none() {
        Logger::info(format!(
            "Found a referenced table/view {ref_name} that doesn't exists"
        ));
        return vec;
    }
    let ref_index = ref_index.unwrap();

    if ref_index > tab_index {
        let org_ref = vec.remove(ref_index);
        vec.insert(tab_index, org_ref);
    }

    return vec;
}
