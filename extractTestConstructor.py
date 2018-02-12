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
        print 'Retrieving rows ...'
        row = cur.fetchone()
        if row is None:
            break
        rows.append(row)
    print 'Retrieved ' + str(len(rows)) + 'rows'
    return rows


def print_row(row):
    print 'Printing row of ' + str(len(rows)) + ' elements'
    temp = []
    for i in range(0, len(row)):
        item = row[i]
        # if type(item) is str:
        #     temp.append("'" + item + "''")
        # else:
        #     temp.append(str(item))
        if isinstance(item, numbers.Number):
            # temp.append(str(item))
            if (i+1 in [1, 3, 6, 7, 11, 12, 18, 19, 22, 23]):
                temp.append(str(item) + 'L')
            elif (i+1 in [2, 4, 8, 20, 24]):
                if item == 1:
                    temp.append('Orientation.FORWARD')
                elif item == -1:
                    temp.append('Orientation.REVERSE')
            elif i+1 == 9:
                if item == 1:
                    temp.append('LocusType.DELETION');
                elif item == 2:
                    temp.append('LocusType.SNP')
                elif item == 3:
                    temp.append('LocusType.INSERTION')
                elif item == 4:
                    temp.append('LocusType.LONGER_ON_CONTIG')
                elif item == 5:
                    temp.append('LocusType.EQUAL_ON_CONTIG')
                elif item == 6:
                    temp.append('LocusType.SHORTER_ON_CONTIG')
                elif item == 7:
                    temp.append('LocusType.FUZZY')
            else:
                temp.append(str(item))
        elif item is None:
            temp.append('null')
        else:
            temp.append('"' + str(item) + '"')
    print 'new SubSnpCoreFields(' + ', '.join(temp) + ');'
    # print 'INSERT INTO ' + table + ' VALUES (' + ', '.join(temp) + ');'

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

with open('query.sql', 'r') as query_file:
    query = query_file.read().replace('$schema', schema) + variant_id
    print 'Executing query ...'
    rows = execute_query(conn, query)
    for row in rows:
        print_row(row)
# conn.close()
