import os

import polars as pl
import trino.dbapi

TRINO_HOST = os.environ.get("TRINO_HOST")
TRINO_USER = os.environ.get("TRINO_USER")


def df_from_db(
    query,
    struct_cols=None,
    host=TRINO_HOST,
    user=TRINO_USER,
    catalog="iceberg",
    **df_kwargs,
):
    with trino.dbapi.connect(host=host, user=user, catalog=catalog) as conn:
        df = df_from_db_conn(query, conn, struct_cols, **df_kwargs)
    return df


def _ingest_row(data, row, struct_cols_idc):
    for i in struct_cols_idc:
        if row[i] is not None:
            row[i] = {n: getattr(row[i], n) for n in row[i]._names}
    data.append(row)


def df_from_db_conn(query, conn, struct_cols=None, **df_kwargs):
    if struct_cols is None:
        struct_cols = []
    data = []

    cur = conn.cursor()
    cur.execute(query)
    col_names = [desc.name for desc in cur.description]
    struct_cols_idc = [i for i, n in enumerate(col_names) if n in struct_cols]
    for row in cur:
        _ingest_row(data, row, struct_cols_idc)
    cur.close()
    df = pl.DataFrame(data, schema=col_names, orient="row", **df_kwargs)
    return df
