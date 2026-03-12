import psycopg2
from psycopg2 import OperationalError, DatabaseError
from psycopg2.extensions import connection
from typing import List, Tuple, Optional

# Criar conexão com o PostgreSQL
def get_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str
) -> connection:
    try:
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=10
        )
    except OperationalError as e:
        print("Erro ao conectar no PostgreSQL.")
        raise

# Executar comandos SQL
def execute_query(
    conn: connection,
    query: str,
    params: Optional[Tuple] = None,
    return_data: bool = True
) -> List[Tuple]:

    cur = None
    try:
        cur = conn.cursor()

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        if return_data:
            result = cur.fetchall()
        else:
            result = []

        conn.commit()
        
        return result

    except DatabaseError:
        conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
