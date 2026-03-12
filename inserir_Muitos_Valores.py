from typing import List, Tuple
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from psycopg2 import DatabaseError

# Inserção de múltiplos valores em uma tabela
def insert_many_values(
    conn: connection,
    table_name: str,
    columns: Tuple[str, ...],
    values: List[Tuple],
    batch_size: int = 1500
) -> None:

    cols = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({cols}) VALUES %s"

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                sql,
                values,
                page_size=batch_size
            )
        conn.commit()

    except DatabaseError as e:
        conn.rollback()
        print("Erro ao inserir dados em lote.")
        print(e)
        raise


