import time
import pickle
import pandas as pd
from io import BytesIO
from apiclient.http import MediaInMemoryUpload
from dsclient.client import ClientBase
from googleapiclient.errors import HttpError


class Client(ClientBase):

    __ENDPOINT_GCS = "https://www.googleapis.com/auth/devstorage.read_write"
    __API_NAME = "storage"
    __API_VERSION = "v1"

    def __init__(self, project_id, account_email=None, keyfile_path=None):

        super(Client, self).__init__(project_id, account_email, keyfile_path)
        self._gsservice = super(Client, self)._build_service(Client.__ENDPOINT_GCS,
                                                             Client.__API_NAME,
                                                             Client.__API_VERSION)

    def _parse_uri(self, uri):

        if not uri.startswith("gs://"):
            raise Exception("uri({0}) must start with 'gs://'".format(uri))

        path = uri.replace("gs://", "")
        bucket, obj = path.split("/", 1)
        return bucket, obj

    def write(self, obj, uri, mimetype='application/octet-stream'):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(obj, mimetype=mimetype))
        resp = req.execute()
        return resp

    def read_csv(self, uri, sep=",", header="infer"):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.get_media(bucket=bucket, object=file_path)
        resp = req.execute()
        df = pd.read_csv(BytesIO(resp), sep=sep, header=header)
        return df

    def write_csv(self, df, uri, sep=","):

        bucket, file_path = self._parse_uri(uri)
        buf = BytesIO()
        df.to_csv(buf, index=False, sep=sep)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(buf.getvalue(), mimetype='text/csv'))
        resp = req.execute()

    def read_blob(self, uri):

        bucket, file_path = self._parse_uri(uri)
        req = self._gsservice.objects().get_media(bucket=bucket, object=file_path)
        resp = req.execute()
        blob = pickle.loads(resp)
        return blob

    def write_blob(self, blob, uri):

        bucket, file_path = self._parse_uri(uri)
        dump = pickle.dumps(blob)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(dump, mimetype='application/octet-stream'))
        resp = req.execute()

    def write_text(self, text, uri):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(text, mimetype='text/plain'))
        resp = req.execute()

    def write_figure(self, figure, uri, image_type="png"):

        bucket, file_path = self._parse_uri(uri)
        buf = BytesIO()
        figure.savefig(buf, format=image_type)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(buf.getvalue(), mimetype='image/'+image_type))
        resp = req.execute()

    def delete_object(self, uri):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        objects.delete(bucket=bucket, object=file_path).execute()

    def get_bucket(self, bucket):

        buckets = self._gsservice.buckets()
        try:
            bucket_ = buckets.get(bucket=bucket).execute()
        except HttpError as e:
            if e.resp.status == 404:
                return None
            raise

        return bucket_

    def list_bucket(self, projection=None, pageToken=None, prefix=None, maxResults=None):

        buckets = self._gsservice.buckets()
        bucket_list = buckets.list(project=self._project_id).execute()
        return bucket_list

    def create_bucket(self, bucket, location=None, storage_class=None, projection=None):

        body = {
            "name": bucket
        }

        if location is not None:
            body["location"] = location

        buckets = self._gsservice.buckets()
        bucket_ = buckets.insert(project=self._project_id,
                                body=body, projection=projection).execute()
        return bucket_

    def delete_bucket(self, bucket):

        buckets = self._gsservice.buckets()
        buckets.delete(bucket=bucket).execute()
