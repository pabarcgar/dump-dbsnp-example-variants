import psycopg2
import numbers
import argparse


dbname='dbsnp_150'
schema='dbsnp_chicken_9031'
id=13677177
host=''
user=''

def register_New_Date():
    # Cast PostgreSQL Date as Python string
    # Reference:
    # 1. http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.new_type
    # 2. http://initd.org/psycopg/docs/advanced.html#type-casting-from-sql-to-python
    # 1082 is OID for DATE type.
    NewDate = psycopg2.extensions.new_type((1082,), 'DATE', psycopg2.STRING)
    psycopg2.extensions.register_type(NewDate)

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

register_New_Date()
conn = connect_db(dbname)

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

# def print_query_result(query_rs):
#

def execute_direct_query(schema, table, where):
    query = 'SELECT * FROM ' + schema + '.' + table + ' WHERE ' + where
    for row in execute_query(conn, query):
        print_row(row, table)
    print ''

def execute_fk_query(schema, table, where, fk_query):
    for row in execute_query(conn, fk_query)

execute_direct_query(schema, 'snp', 'snp_id='+str(id))
execute_direct_query(schema, 'b150_snpcontigloc', 'snp_id='+str(id))
execute_direct_query(schema, 'snpsubsnplink', 'snp_id='+str(id))
execute_direct_query(schema, 'subsnp', 'snp_id='+str(id))
execute_direct_query(schema, 'b150_snphgvslink', 'snp_link='+str(id))

# # query_rs = 'SELECT * FROM ' + schema + '.subsnp WHERE snp_id=' + str(id)
#
#
# query_rs = 'SELECT * FROM ' + schema + '.snpsubsnplink WHERE snp_id=' + str(id)
# for row in execute_query(conn, query_rs):
#     # temp = []
#     # temp = [ str(item) for item in line]
#     print_row(row)

conn.close()
print 'Finished'
