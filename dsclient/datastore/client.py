import numpy as np
import pandas as pd
from .. base import ClientBase


class Client(ClientBase):

    __ENDPOINT_GDS = "https://www.googleapis.com/auth/datastore"
    __API_NAME = "datastore"
    __API_VERSION = "v1"

    def __init__(self, project_id, keyfile_path=None, account_email=None):

        super(Client, self).__init__(project_id, keyfile_path, account_email)
        self._dscredentials, self._dsservice = super(Client, self)._build_service(Client.__API_NAME,
                                                                                  Client.__API_VERSION,
                                                                                  Client.__ENDPOINT_GDS)
    def get_dsservice(self):

        return self._dsservice

    def gql(self, query):

        projects = self._dsservice.projects()
        body = {
            "gqlQuery": {
                "queryString": query,
                "allowLiterals": True
            }
        }
        req = projects.runQuery(projectId=self._project_id, body=body)
        resp = self._try_execute(req)

        entities = resp["batch"]["entityResults"]
        if len(entities) == 0:
            return pd.DataFrame()

        def extract_cols_value(k, v):
            if "stringValue" in v:
                return (k, "s", v["stringValue"])
            elif "integerValue" in v:
                return (k, "i", v["integerValue"])
            elif "doubleValue" in v:
                return (k, "f", v["doubleValue"])
            elif "booleanValue" in v:
                return (k, "b", v["booleanValue"])
            elif "timestampValue" in v:
                return (k, "d", v["datetimeValue"])
            elif "nullValue" in v:
                return (k, "n", None)
            elif "keyValue" in v:
                keypath = v["keyValue"]["path"][0]
                value = keypath["name"] if keypath["id"] is None else keypath["id"]
                dtype = "s" if keypath["id"] is None else "i"
                return (k, dtype, value)
            else:
                return (k, "z", None)

        def judge_type(t):
            dtype = object

            if "z" in t:
                return (object, "z")
            np.delete(t, np.where(t=="n"))
            if len(t) == 0:
                return (object, "s")
            if len(t) == 1:
                if "i" in t:
                    return (np.int64, "i")
                elif "f" in t:
                    return (np.float64, "f")
                elif "b" in t:
                    return (np.bool, "b")
                elif "d" in t:
                    return (object, "d")
                else:
                    return (object, "s")

            if len(t) == 2 and "i" in t and "f" in t:
                return (np.float64, "f")

            return (object, "s")

        def convert_dataframe(entities, calc_dtype=False):

            rows = [[extract_cols_value(k, v) for k, v in entity["entity"]["properties"].items()] for entity in entities]
            #vals = [[col[2] for col in row] for row in rows]
            vals = [[col[2] for col in row] for row in rows]
            cols = entities[0]["entity"]["properties"].keys()
            df = pd.DataFrame(vals, columns=cols)

            if calc_dtype:
                types = [[col[1] for col in row] for row in rows]
                df_types = pd.DataFrame(types, columns=cols)
                dtypes = [(col, judge_type(df_types[col].unique())) for col in cols]
                return df, dtypes

            return df

        kind_names = [{"name": kind} for kind in set([p["kind"]  for entity in entities for p in entity["entity"]["key"]["path"]])]

        df, dtypes = convert_dataframe(entities, True)

        scols = [d[0] for d in dtypes if d[1][1] == "s"]
        ncols = [d[0] for d in dtypes if d[1][1] == "i" or d[1][1] == "f"]
        bcols = [d[0] for d in dtypes if d[1][1] == "b"]
        tcols = [d[0] for d in dtypes if d[1][1] == "d"]

        #df[scols] = df[scols].astype(str)
        df[bcols] = df[bcols].astype(bool)
        df[ncols] = df[ncols].apply(pd.to_numeric)
        df[tcols] = df[tcols].apply(pd.to_datetime)

        df_list = [df]
        while resp["batch"]["moreResults"] == "NOT_FINISHED":
            body = {
                "query": {
                    "startCursor": resp["batch"]["endCursor"],
                    "kind": kind_names
                }
            }
            req = projects.runQuery(projectId=self._project_id, body=body)
            resp = self._try_execute(req)
            entities = resp["batch"]["entityResults"]
            if len(entities) == 0:
                return pd.concat(df_list)

            df = convert_dataframe(entities)

            #df[scols] = df[scols].astype(str)
            df[bcols] = df[bcols].astype(bool)
            df[ncols] = df[ncols].apply(pd.to_numeric)
            df[tcols] = df[tcols].apply(pd.to_datetime)

            df_list.append(df)

        dfs = pd.concat(df_list)
        return dfs

    def get(self, keys):

        pass

    def put(self, entities, keycol):

        pass
