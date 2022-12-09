import psycopg2
from sql_queries import create_table_queries,drop_table_queries
def create_database(name):
    try:
        conn=psycopg2.connect("host=localhost dbname=Dataengineering user=postgres password=Bas617448")
    except psycopg2.Error() as e:
        print("error while creating a connection\n")
        print(e)
    conn.set_session(autocommit=True)    
    try:
        cur=conn.cursor()
    except psycopg2.Error() as e:
        print("error while creating the cursor\n")
        print(e)
        
    cur.execute(f"DROP DATABASE IF EXISTS {name}")
    conn.commit()
    cur.execute(f"CREATE DATABASE {name}")
    conn.commit()
    cur.close()
    conn.close()
    
    try:
        conn=psycopg2.connect(f"host=localhost dbname={name} user=postgres password=Bas617448")
    except psycopg2.Error() as e:
        print("error while connecting with new database \n")
        print(e)
        
    try:
        cur=conn.cursor()
    except psycopg2.Error() as e:
        print("error while creating the cursor for the new database \n")
        print(e)
    return conn,cur

def drop_tables(conn,cur):
    try:
        for i in drop_table_queries:
            cur.execute(i)
    except psycopg2.Error() as e:
        print("error while dropping the tables")
        print(e)
    finally:
        conn.commit()

def create_tables(conn,cur):
    try:
        for i in create_table_queries:
            cur.execute(i)
    
    except psycopg2.Error() as e:
        print("error while creating the tables")
        print(e)
    finally:
        conn.commit()


def main():
    
    conn,cur=create_database("sparkifydb")
    
    drop_tables(conn, cur)
    
    create_tables(conn, cur)
    
    conn.close()
    
if __name__ =="__main__":
    main()
    
    
    
    
    