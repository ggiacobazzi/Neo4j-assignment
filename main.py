import subprocess

from neo4j import __version__ as neo4j_version
from neo4j import GraphDatabase
from pathlib import Path
from subprocess import *
import time

menu_options = {
    1: 'Connect to neo4j',
    2: 'Check connection',
    3: 'Create database',
    4: 'Setup environment',
    5: 'Show databases',
    6: 'Drop database',
    7: 'Distributed query',
    8: 'Drop indexes',
    9: 'Count nodes',
    10: 'Exit',
}


class ChildProcess:

    def __init__(self, process):
        self.__process = process

    def kill(self):
        self.__process.kill()


class Neo4jConnection:

    def __init__(self, uri=None, user=None, pwd=None, silent=None):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            if not silent:
                print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def check_connection(self, silent):
        assert self.__driver is not None, "Driver not initialized!"
        if not silent:
            print("CONNECTED")

    def query(self, query, db=None, params=None, total_time=0):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            start = time.time()
            response = list(session.run(query)) if params is None else list(session.run(query, param=params))
            total_time += time.time() - start
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response, total_time


class Neo4JDB:
    db = Neo4jConnection(silent=True)


def print_menu():
    for key in menu_options.keys():
        print(key, '--', menu_options[key])


def open_neo():
    # open neo4j console
    neo4j_path = 'C:\\Users\\gabri\\Desktop\\Databases for Big Data\\Neo4j\\neo4j-community-4.3.7\\bin\\neo4j.bat'
    neo4j_param = "console"
    p = subprocess.Popen([neo4j_path, neo4j_param], shell=True, stdout=PIPE, stderr=PIPE)
    return p


def connect():
    # open neo4j connection
    neo4jdb.db = Neo4jConnection(uri="bolt://localhost:11007", user="neo4j", pwd="abc")
    print("Connected")
    connected = True
    return neo4jdb, connected


def check_connection(silent=None):
    try:
        if not silent:
            print("Neo4j v. " + neo4j_version)
            print('Neo4j opened')
        neo4jdb.db.check_connection(silent=True)
    except Exception:
        if not silent:
            print('NOT CONNECTED')
        return False
    return True


def create_database():
    db_name = input("Input"
                    " a database name: ")
    db_query = "CREATE OR REPLACE DATABASE " + db_name
    neo4jdb.db.query(db_query)
    print("Created database " + db_name)
    show_databases()


def setup_environment():
    db_query0 = "CREATE OR REPLACE DATABASE db0"
    neo4jdb.db.query(db_query0)

    db_query1 = "CREATE OR REPLACE DATABASE db1"
    neo4jdb.db.query(db_query1)

    db_query2 = "CREATE OR REPLACE DATABASE db2"
    neo4jdb.db.query(db_query2)
    show_databases()

    print("----------------------------------------------")

    # db0 setup
    print("Setup db0")
    db_query0 = load_setup_db("db0").split("\n\n")
    total_time = 0
    for query in db_query0:
        time_elapsed_query = 0
        res, time_elapsed_query = neo4jdb.db.query(query, db='db0', total_time=time_elapsed_query)
        total_time += time_elapsed_query

    print("Time elapsed in ms:" + str(total_time))

    db_query0 = "MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)" \
                " RETURN s.companyName AS Supplier, p.productName AS Product, c.categoryName AS Category LIMIT 5"
    total_time = 0
    query_res, total_time = neo4jdb.db.query(db_query0, db='db0', total_time=total_time)
    print_res(query_res)
    print("Time elapsed in ms:" + str(total_time))
    print("----------------------------------------------")

    # db1 setup
    print("Setup db1...")
    db_query1 = load_setup_db("db1").split("\n\n")
    eu_countries = ['Germany', 'UK', 'Sweden', 'France', 'Spain', 'Switzerland', 'Austria', 'Italy', 'Portugal',
                    'Ireland', 'Belgium', 'Norway', 'Denmark', 'Finland']
    total_time = 0

    for query in db_query1:
        time_elapsed_query = 0
        res, time_elapsed_query = neo4jdb.db.query(query, db='db1', params=eu_countries, total_time=time_elapsed_query)
        total_time += time_elapsed_query
    print("Time elapsed in ms:" + str(total_time))

    db_query1 = "MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product) " \
                "RETURN c.companyName AS Customer, c.country AS CustomerCountry, o.orderID AS Order, p.productID AS " \
                "Product LIMIT 5"
    total_time = 0
    query_res, total_time = neo4jdb.db.query(db_query1, db='db1', total_time=total_time)
    print_res(query_res)
    print("Time elapsed in ms:" + str(total_time))
    print("----------------------------------------------")

    # db2 setup
    print("Setup db2...")
    db_query2 = load_setup_db("db2").split("\n\n")
    americas = ['Mexico', 'Canada', 'Argentina', 'Brazil', 'USA', 'Venezuela']

    total_time = 0
    for query in db_query2:
        time_elapsed_query = 0
        res, time_elapsed_query = neo4jdb.db.query(query, db='db2', params=americas, total_time=time_elapsed_query)
        total_time += time_elapsed_query

    print("Time elapsed in ms:" + str(total_time))

    db_query2 = "MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)" \
                "RETURN c.companyName AS Customer, c.country AS CustomerCountry, o.orderID AS Order, p.productID AS " \
                "Product LIMIT 5;"
    total_time = 0
    query_res, total_time = neo4jdb.db.query(db_query2, db='db2', total_time=total_time)
    print_res(query_res)
    print("Time elapsed in ms:" + str(total_time))
    print("----------------------------------------------")


def load_setup_db(db):
    setup_db_path = "setup_" + db + ".txt"
    query = Path(setup_db_path).read_text()
    return query


def destroy_environment():
    db_query = "DROP DATABASE db0 IF EXISTS"
    neo4jdb.db.query(db_query)

    db_query = "DROP DATABASE db1 IF EXISTS"
    neo4jdb.db.query(db_query)

    db_query = "DROP DATABASE db2 IF EXISTS"
    neo4jdb.db.query(db_query)


def show_databases():
    databases, total_time = neo4jdb.db.query("SHOW DATABASES")
    print("Databases")
    print_res(databases)


def drop_database():
    db_name = input("Input a database name to drop: ")
    db_query = "DROP DATABASE " + db_name + " IF EXISTS"
    neo4jdb.db.query(db_query)
    print("Dropped database " + db_name + "!")
    show_databases()


def drop_indexes():
    query = "CALL apoc.schema.assert({},{})"
    neo4jdb.db.query(query)


def count_nodes():
    query_path = "count_nodes.txt"
    query = Path(query_path).read_text()
    query_res, total_time = neo4jdb.db.query(query, db='db0')
    print_res(query_res)

    query_res, total_time = neo4jdb.db.query(query, db='db1')
    print_res(query_res)

    query_res, total_time = neo4jdb.db.query(query, db='db2')
    print_res(query_res)


def distributed_query():
    setup_db_query = "distributed_query.txt"
    query = Path(setup_db_query).read_text()
    total_time = 0
    query_res, total_time = neo4jdb.db.query(query, total_time=total_time)

    print_res(query_res)

    print("Time elapsed in ms: " + str(total_time))

    setup_db_query = "distributed_query2.txt"
    query = Path(setup_db_query).read_text()
    query_res, total_time = neo4jdb.db.query(query, total_time=total_time)

    print_res(query_res)

    print("Time elapsed in ms: " + str(total_time))


def print_res(query_res):
    for res in query_res:
        print(res)
    
    
def application_menu():
    while True:
        print_menu()
        option = ''
        try:
            option = int(input('Enter your choice: '))
        except:
            print('Wrong input. Please enter a number ...')
        if option == 1:
            connect()
        elif option == 2:
            check_connection(silent=False)
        elif option == 3:
            create_database()
        elif option == 4:
            setup_environment()
        elif option == 5:
            show_databases()
        elif option == 6:
            drop_database()
        elif option == 7:
            distributed_query()
        elif option == 8:
            drop_indexes()
        elif option == 9:
            count_nodes()
        elif option == 10:
            print('Thanks message before exiting')
            if check_connection(silent=True):
                destroy_environment()
            proc.kill()
            exit()
        else:
            print('Invalid option. Please enter a number between 1 and 10.')


if __name__ == '__main__':
    proc = ChildProcess(open_neo())
    neo4jdb = Neo4JDB()
    application_menu()
