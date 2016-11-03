import numpy as np
import pandas as pd


def convert_dtype2bqfield(column_dtype):

    if "float" in column_dtype:
        return "float"
    elif "int" in column_dtype:
        return "integer"
    elif "bool" in column_dtype:
        return "boolean"
    elif "str" in column_dtype or "object" in column_dtype:
        return"string"
    else:
        return "string"

def convert_df2bqschema(df):

    fields = [{"name": name,
               "type": convert_dtype2bqfield(str(df[name].dtype))
              } for name in df.columns]
    return {"fields": fields}


class Schema(object):

    def __init__(self, schema):

        dtype = {}
        stype = {}
        cols  = []
        scols = []
        ncols = []
        bcols = []
        tcols = []

        for field in schema["fields"]:
            name = field["name"]
            rtype = field["type"].lower()
            if rtype == "string":
                dtype[name] = object
                stype[name] = object
                scols.append(name)
            elif rtype == "integer":
                dtype[name] = np.int64
                ncols.append(name)
            elif rtype == "float":
                dtype[name] = np.float64
                ncols.append(name)
            elif rtype == "boolean":
                dtype[name] = np.bool
                bcols.append(name)
            elif rtype == "timestamp":
                tcols.append(name)
            else:
                dtype[name] = object
                scols.append(name)
            cols.append(name)

        self._dtype = dtype
        self._stype = stype
        self._cols  = cols
        self._scols = scols
        self._ncols = ncols
        self._bcols = bcols
        self._tcols = tcols

    def get_dtype(self):

        return self._dtype

    def get_object_dtype(self):

        return self._stype

    def update_dtype(self, df):

        if self._scols:
            df[self._scols] = df[self._scols].astype(str)
        if self._bcols:
            df[self._bcols] = df[self._bcols].astype(bool)
        if self._ncols:
            df[self._ncols] = df[self._ncols].apply(pd.to_numeric)
        if self._tcols:
            df[self._tcols] = df[self._tcols].apply(pd.to_datetime)
        #df[num_clmns] = df[num_clmns].convert_objects(convert_numeric=True)
        #df[tim_clmns] = df[tim_clmns].convert_objects(convert_dates=True)
        return df

    def to_dataframe(self, bq_rows):

        rows = [[col["v"] for col in row["f"]] for row in bq_rows]
        df = pd.DataFrame(rows, columns=self._cols)
        df = self.update_dtype(df)
        return df
