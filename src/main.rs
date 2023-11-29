use clap::Parser;
use dbdump::std_writer::StdWriter;
use sqlx::mysql::MySqlPoolOptions;
use url::Url;

use dbdump::*;

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

    #[arg(long = "user", required = false)]
    username: Option<String>,

    #[arg(long = "pass", required = false)]
    password: Option<String>,

    /// Extract schema only
    #[arg(short = 'd', long = "no-data", default_value_t = false)]
    exclude_data: bool,

    /// Extract ddl only
    #[arg(short = 'n', long = "no-ddl", default_value_t = false)]
    exclude_ddl: bool,

    /// Filename to write output to
    #[arg(short, long = "output-file", required = false)]
    output_file: Option<String>,

    /// Thread count for data extraction
    /// Default is 1
    /// 0 means use the number of cores
    #[arg(long = "thread-count", required = false, default_value_t = 1)]
    thread_count: usize,

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

    let mut url = Url::parse(&args.url).expect("Invalid url, unable to parse");
    if let Some(user) = args.username {
        url.set_username(&user).expect("Cannot set username");
    }
    if let Some(pass) = args.password {
        url.set_password(Some(pass.as_str()))
            .expect("Cannot set password");
    }

    let mut writer = StdWriter::new(args.output_file);
    let schema = if let Some(schema) = args.schema {
        schema
    } else {
        url.path()
            .split_once('/')
            .expect("Unable to obtain the schema. Either include the schema name as part of the url or pass it using the --schema argument")
            .1
            .to_string()
    };

    //
    // Compute number of threads to use
    //
    let thread_count = 
    if args.thread_count == 0 {
        let num_cpus = num_cpus::get();
        if num_cpus > 1 { num_cpus - 1 } else { num_cpus }
    } else {
        args.thread_count
    };

    //
    // Create a pool of connections.
    // Probably overkill as we currently only use one connection
    //
    let pool = MySqlPoolOptions::new()
        .max_connections(thread_count as u32 +1)
        .connect(&url.to_string())
        .await?;

    //
    // Start writing the 'file', header and whatever other statements required
    //
    write_header(&mut writer, &schema, &args.url);
    write_prefix(
        &mut writer,
        &schema,
        args.renamed_schema_name,
        args.create_schema,
        true,
    );

    //
    // The order of the writes should be as follows:
    // 1. Base tables
    // 2. Views (require tables)
    // 3. Stored procedures and functions
    // 4. Triggers
    //
    if !args.exclude_ddl {
        export_tables(&pool, &mut writer, &schema).await?;
        export_views(&pool, &mut writer, &schema).await?;
        export_stored_procs(&pool, &mut writer, &schema).await?;
        export_functions(&pool, &mut writer, &schema).await?;
        export_triggers(&pool, &mut writer, &schema).await?;
    }

    //
    // After the DDL is written write the data.
    // We turn off constraints until after the data is loaded so that
    //   we don't run into any constraint violations during the load
    //
    if !args.exclude_data {
        export_data(
            &pool,
            &mut writer,
            thread_count,
            &schema,
            args.single_row_inserts,
            args.skip_unknown_datatypes,
        )
        .await?;
    }

    write_postfix(&mut writer, true);
    write_footer(&mut writer);

    Ok(())
}
