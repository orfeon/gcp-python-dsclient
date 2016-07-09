import os
import time
from .. import bigquery
from .. import storage
from .. schema import Schema


class Client(bigquery.Client, storage.Client):

    def __init__(self, project_id, account_email=None, keyfile_path=None):

        super(Client, self).__init__(project_id, account_email, keyfile_path)

    def lquery(self, query, dataset_id=None, bucket=None):

        table_id = "tmp_{0}_{1}_{2}".format(os.uname()[1], os.getpid(), int(time.time()))

        if bucket is None:
            bucket = table_id
            self.create_bucket(bucket)
        if dataset_id is None:
            dataset_id = table_id
            self.create_dataset(dataset_id=dataset_id, expiration_ms=3600000)

        gs_uri     = "gs://{0}/{1}.csv".format(bucket, table_id)
        table_name = "{0}.{1}".format(dataset_id, table_id)

        self.insert(query, table_name)
        #expirationTime = int(time.time()) * 1000 + 360000
        table = self.extract(table_name, gs_uri)
        schema = Schema(table["schema"])

        self.delete_table(table_name)
        if dataset_id == table_id:
            self.delete_dataset(dataset_id)

        df = self.read_csv(gs_uri)
        df = schema.update_dtype(df)

        self.delete_object(gs_uri)
        if bucket == table_id:
            self.delete_bucket(bucket)

        return df
