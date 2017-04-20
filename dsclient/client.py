import os
import time
from . import bigquery
from . import storage
from . import datastore
from . import compute

class Client(bigquery.Client, storage.Client, datastore.Client, compute.Client):

    def __init__(self, project_id, keyfile_path=None, account_email=None):

        super(Client, self).__init__(project_id, keyfile_path, account_email)

    def lquery(self, query, dataset_id=None, bucket=None):

        table_id = "tmp_{0}_{1}_{2}".format(os.uname()[1].replace("-","_"), os.getpid(), int(time.time()))

        if dataset_id is None:
            dataset_id = table_id
            self.create_dataset(dataset_id=dataset_id, expiration_ms=3600000)
        table_name = "{0}.{1}".format(dataset_id, table_id)
        try:
            self.query(query, table_name=table_name)
        except:
            if dataset_id == table_id:
                self.delete_dataset(dataset_id)
            raise

        #expirationTime = int(time.time()) * 1000 + 360000
        if bucket is None:
            bucket = table_id
            self.create_bucket(bucket)
        gs_uri = "gs://{0}/{1}.csv".format(bucket, table_id)
        try:
            table = self.extract(table_name, gs_uri)
        except:
            if bucket == table_id:
                self.delete_bucket(bucket)
            self.delete_table(table_name)
            if dataset_id == table_id:
                self.delete_dataset(dataset_id)
            raise

        self.delete_table(table_name)
        if dataset_id == table_id:
            self.delete_dataset(dataset_id)

        schema = Schema(table["schema"])
        dtype = schema.get_object_dtype()
        df = self.read_csv(gs_uri, dtype=dtype)
        df = schema.update_dtype(df)

        self.delete_object(gs_uri)
        if bucket == table_id:
            self.delete_bucket(bucket)

        return df
