
import datetime as dt
import json

import polars as pl


def _json_default(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    else:
        return repr(value)


def save_dict(d, path):
    path.write_text(json.dumps(d, default=_json_default, indent=4))


def load_dict(path):
    plot_d = json.loads(path.read_text())
    return plot_d


def df_from_data_d(data_d):
    return pl.DataFrame(data_d, schema_overrides={"date": pl.Datetime})
