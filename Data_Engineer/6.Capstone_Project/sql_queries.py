import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
CSV_DATA_1 = config.get('S3', 'CSV_DATA_1')
CSV_DATA_2 = config.get('S3', 'CSV_DATA_2')
PARQUET_DATA = config.get('S3', 'PARQUET_DATA')
IAM_ROLE = config.get('IAM', 'ARN')


# DROP TABLES
staging_company_table_drop = "DROP TABLE IF EXISTS stage_company"
stage_stocks_table_drop = "DROP TABLE IF EXISTS stage_stocks"

fact_stocks_table_drop = "DROP TABLE IF EXISTS fact_stocks"
dim_index_table_drop = "DROP TABLE IF EXISTS dim_index"
dim_company_table_drop = "DROP TABLE IF EXISTS dim_company"
dim_location_table_drop = "DROP TABLE IF EXISTS dim_location"
dim_exchange_table_drop = "DROP TABLE IF EXISTS dim_exchange"





# CREATE TABLES

staging_company_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_company(
    exchange VARCHAR(255),
    symbol VARCHAR(255),
    shortname VARCHAR(255),
    longname VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255),
    current_price FLOAT,
    marketcap FLOAT,
    ebitda FLOAT,
    revenue_growth FLOAT,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    fulltime_employees FLOAT,
    long_business_summary VARCHAR(3000),
    weight FLOAT
    ) 
""")



staging_stocks_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_stocks(
    date VARCHAR(255) NOT NULL PRIMARY KEY,
    adjusted_closing_price VARCHAR(255),
    close_price VARCHAR(255),
    high_price VARCHAR(255),
    low_price VARCHAR(255),
    open_price VARCHAR(255),
    volume VARCHAR(255),
    symbol VARCHAR(255)
    ) 
""")

fact_stocks_table_create= ("""
    CREATE TABLE IF NOT EXISTS fact_stocks(
    date DATE NOT NULL PRIMARY KEY,
    adjusted_closing_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    open_price FLOAT,
    volume FLOAT,
    symbol VARCHAR(255)
    ) 
""")



dim_index_table_create= ("""
    CREATE TABLE IF NOT EXISTS dim_index(
    date DATE NOT NULL PRIMARY KEY,
    sp500_index FLOAT
    ) 
""")

dim_location_table_create= ("""
    CREATE TABLE IF NOT EXISTS dim_location(
    location_id INT IDENTITY(0, 1) NOT NULL PRIMARY KEY,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255)
    ) 
""")

dim_exchange_table_create= ("""
    CREATE TABLE IF NOT EXISTS dim_exchange(
    exchange_id INT IDENTITY(0, 1) NOT NULL PRIMARY KEY,
    exchange VARCHAR(255)
    ) 
""")

dim_company_table_create= ("""
    CREATE TABLE IF NOT EXISTS dim_company(
    symbol VARCHAR(255) NOT NULL PRIMARY KEY,
    shortname VARCHAR(255),
    longname VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255),
    current_price FLOAT,
    marketcap FLOAT,
    ebitda FLOAT,
    revenue_growth FLOAT,
    fulltime_employees FLOAT,
    weight FLOAT,
    exchange_id INT REFERENCES dim_exchange(exchange_id),
    location_id INT REFERENCES dim_location(location_id)
    ) 
""")


# STAGING TABLES

stage_company_copy = ("""
    copy stage_company from {bucket}
    iam_role '{iam_role}'
    region 'us-west-2'
    FORMAT AS CSV
    ignoreheader 1
""").format(bucket = CSV_DATA_1, iam_role = IAM_ROLE)

stage_stocks_copy = ("""
    copy stage_stocks from {bucket}
    iam_role '{iam_role}'
    FORMAT AS PARQUET
""").format(bucket = PARQUET_DATA, iam_role = IAM_ROLE)


# FINAL TABLES

dim_index_copy = ("""
    copy dim_index from {bucket}
    iam_role '{iam_role}'
    region 'us-west-2'
    FORMAT AS CSV
    ignoreheader 1
""").format(bucket = CSV_DATA_2, iam_role = IAM_ROLE)

fact_stocks_table_insert = ("""
    INSERT INTO fact_stocks(
    date,
    adjusted_closing_price,
    close_price,
    high_price,
    low_price,
    open_price,
    volume,
    symbol
    )
    SELECT
    CAST(date AS DATE),
    CAST(adjusted_closing_price AS FLOAT),
    CAST(close_price AS FLOAT),
    CAST(high_price AS FLOAT),
    CAST(low_price AS FLOAT),
    CAST(open_price AS FLOAT),
    CAST(volume AS FLOAT),
    symbol
    FROM stage_stocks;
""")

dim_company_table_insert = ("""
    INSERT INTO dim_company(
    symbol,
    shortname,
    longname,
    sector,
    industry,
    current_price,
    marketcap,
    ebitda,
    revenue_growth,
    fulltime_employees,
    weight
    )
    SELECT DISTINCT
    symbol,
    shortname,
    longname,
    sector,
    industry,
    current_price,
    marketcap,
    ebitda,
    revenue_growth,
    fulltime_employees,
    weight
    FROM stage_company;
""")

dim_location_insert = ("""
    INSERT INTO dim_location(
    city,
    state,
    country
    )
    SELECT DISTINCT
    city,
    state,
    country
    FROM stage_company;
""")

dim_exchange_insert = ("""
    INSERT INTO dim_exchange(
    exchange
    )
    SELECT DISTINCT 
    exchange
    FROM stage_company;
""")


# QUERY LISTS

drop_table_queries = [staging_company_table_drop, stage_stocks_table_drop, fact_stocks_table_drop, dim_index_table_drop, dim_company_table_drop, dim_location_table_drop, dim_exchange_table_drop]
create_table_queries = [staging_company_table_create, staging_stocks_table_create, fact_stocks_table_create, dim_index_table_create, dim_location_table_create, dim_exchange_table_create, dim_company_table_create]
copy_table_queries = [stage_company_copy, stage_stocks_copy, dim_index_copy]
insert_table_queries = [fact_stocks_table_insert, dim_location_insert, dim_exchange_insert, dim_company_table_insert]