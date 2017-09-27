import psycopg2
import numbers
import argparse


def connect_db(dbname):
    conn = psycopg2.connect(host=host, user=user, dbname=dbname)
    return conn


def execute_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    # row = cur.fetchone()
    rows =[]
    while True:
        row = cur.fetchone()
        if row is None:
            break
        rows.append(row)
    return rows


def print_row(row, table):
    temp = []
    for item in row:
        # if type(item) is str:
        #     temp.append("'" + item + "''")
        # else:
        #     temp.append(str(item))
        if isinstance(item, numbers.Number):
            temp.append(str(item))
        elif item is None:
            temp.append('NULL')
        else:
            temp.append("'" + str(item) + "'")
    print 'INSERT INTO ' + table + ' VALUES (' + ', '.join(temp) + ');'


def execute_direct_query(schema, table, where):
    query = 'SELECT * FROM ' + schema + '.' + table + ' WHERE ' + where
    for row in execute_query(conn, query):
        print_row(row, table)
    print ''


def execute_complex_query(schema, table, query):
    for row in execute_query(conn, query):
        print_row(row, table)
    print ''


def parse_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='hostname', required=True)
    parser.add_argument('-u', '--user', help='user', required=True)
    parser.add_argument('-d', '--dbName', help='db name', required=True)
    parser.add_argument('-s', '--schema', help='db schema', required=True)
    parser.add_argument('-i', '--variantId', help='Variant rs id', required=True)

    args = parser.parse_args()
    return args

# parse cli args
args = parse_cli()
host = args.host
user = args.user
dbname = args.dbName
schema = args.schema
variant_id = args.variantId

#connect to database and execute queries
conn = connect_db(dbname)

query_contig = ('select contig.* '
            	'from ' + schema + '.b150_contiginfo contig '
            	'join ' + schema + '.b150_snpcontigloc loc on loc.ctg_id=contig.ctg_id '
            	'where loc.snp_id = ' + str(variant_id))
execute_complex_query(schema, 'b150_contiginfo', query_contig)

execute_direct_query(schema, 'snp', 'snp_id='+str(variant_id))
execute_direct_query(schema, 'b150_snpcontigloc', 'snp_id='+str(variant_id))
execute_direct_query(schema, 'snpsubsnplink', 'snp_id='+str(variant_id))

query_subsnp = ('select subsnp.* '
                'from ' + schema + '.subsnp '
                'join ' + schema + '.snpsubsnplink on subsnp.subsnp_id = snpsubsnplink.subsnp_id '
                'where snpsubsnplink.snp_id=' + str(variant_id))
execute_complex_query(schema, 'subsnp', query_subsnp)

# execute_direct_query(schema, 'b150_snphgvslink', 'snp_link='+str(variant_id))

query_obsvariation = ('select DISTINCT(obsvariation.*) '
                    	'from dbsnp_shared.obsvariation '
                    	'join ' + schema + '.subsnp on subsnp.variation_id = obsvariation.var_id '
                        'join ' + schema + '.snpsubsnplink  on subsnp.subsnp_id = snpsubsnplink.subsnp_id '
                    	'where snpsubsnplink.snp_id= ' + str(variant_id))
execute_complex_query('dbsnp_shared', 'obsvariation', query_obsvariation)

conn.close()
