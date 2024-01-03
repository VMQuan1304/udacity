import configparser
import psycopg2

def data_quality(conn, cur, dq_checks):
    for dq_check in dq_checks:
        cur.execute(dq_check['test_sql'])
        records = cur.fetchone()

        if dq_check['expected_result'] == records[0]:
            print(f"There are no data in table {dq_check['table']}")
        else:
            print(f"Data quality check pass on table {dq_check['table']}")
            
def data_type(conn, cur, table, test_list):
    expected = test_list[table]
    
    cur.execute(f"SELECT * FROM PG_TABLE_DEF WHERE schemaname = 'public' AND tablename = '{table}';")
    records = cur.fetchall()
    
    
    for record,expect in zip(records, expected):
        if {record[2]: record[3]} == {expect['column']: expect['type']}:
            print("Test type pass on: ")
            print({record[2]: record[3]}, {expect['column']: expect['type']})
        else:
            print("fail")
            print({record[2]: record[3]}, {expect['column']: expect['type']})
    
    
            
            
            
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    dq_checks=[
    {'test_sql': "SELECT COUNT(*) FROM fact_stocks", 'expected_result': 0, 'table':'fact_stocks'},
    {'test_sql': "SELECT COUNT(*) FROM dim_index", 'expected_result': 0, 'table':'dim_index'},
    {'test_sql': "SELECT COUNT(*) FROM dim_company", 'expected_result': 0, 'table':'dim_company'},
    {'test_sql': "SELECT COUNT(*) FROM dim_location", 'expected_result': 0, 'table':'dim_location'},
    {'test_sql': "SELECT COUNT(*) FROM dim_exchange", 'expected_result': 0, 'table':'dim_exchange'},
]
    

    fact_stocks_expected = [{'column':'date', 'type': 'date'},
                            {'column':'adjusted_closing_price', 'type': 'double precision'},
                            {'column':'close_price', 'type': 'double precision'},
                            {'column':'high_price', 'type': 'double precision'},
                            {'column':'low_price', 'type': 'double precision'},
                            {'column':'open_price', 'type': 'double precision'},
                            {'column':'volume', 'type': 'double precision'},
                            {'column':'symbol', 'type': 'character varying(255)'}]
    dim_company_expected = [{'column':'symbol', 'type': 'character varying(255)'},
                              {'column':'shortname', 'type': 'character varying(255)'},
                              {'column':'longname', 'type': 'character varying(255)'},
                              {'column': 'sector','type': 'character varying(255)'},
                              {'column':'industry', 'type': 'character varying(255)'},
                              {'column':'current_price', 'type': 'double precision'},
                              {'column':'marketcap', 'type': 'double precision'},
                              {'column':'ebitda', 'type': 'double precision'},
                              {'column':'revenue_growth', 'type': 'double precision'},
                              {'column':'fulltime_employees', 'type': 'double precision'},
                              {'column':'weight', 'type': 'double precision'},
                              {'column':'exchange_id', 'type': 'integer'},
                              {'column':'location_id', 'type': 'integer'}]

    dim_exchange_expected = [{'column':'exchange_id', 'type': 'integer'},
                              {'column':'exchange', 'type': 'character varying(255)'},]

    dim_index_expected = [{'column':'date', 'type': 'date'},
                          {'column':'sp500_index', 'type': 'double precision'},]

    dim_location_expected = [{'column':'location_id', 'type': 'integer'},
                              {'column':'city', 'type': 'character varying(255)'},
                              {'column':'state', 'type': 'character varying(255)'},
                              {'column':'country', 'type': 'character varying(255)'}]

    test_list = {'fact_stocks': fact_stocks_expected,
                'dim_company': dim_company_expected,
                'dim_exchange': dim_exchange_expected,
                'dim_index': dim_index_expected,
                'dim_location': dim_location_expected
                }
    
    tables = ['fact_stocks', 'dim_company', 'dim_exchange', 'dim_index', 'dim_location']
    
    for table in tables:
        data_type(conn, cur, table, test_list)
    
    
    data_quality(conn, cur, dq_checks)
    conn.close()


if __name__ == "__main__":
    main()
    