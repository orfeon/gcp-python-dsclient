import time
import pickle
import pandas as pd
from io import BytesIO
from apiclient.http import MediaInMemoryUpload
from googleapiclient.errors import HttpError
from .. base import ClientBase


class Client(ClientBase):

    __ENDPOINT_GCS = "https://www.googleapis.com/auth/devstorage.read_write"
    __API_NAME = "storage"
    __API_VERSION = "v1"

    def __init__(self, project_id, keyfile_path=None, account_email=None):

        super(Client, self).__init__(project_id, keyfile_path, account_email)
        self._gscredentials, self._gsservice = super(Client, self)._build_service(Client.__API_NAME,
                                                                                  Client.__API_VERSION,
                                                                                  Client.__ENDPOINT_GCS)

    def _parse_uri(self, uri):

        if not uri.startswith("gs://"):
            raise Exception("uri({0}) must start with 'gs://'".format(uri))

        path = uri.replace("gs://", "")
        bucket, obj = path.split("/", 1)
        return bucket, obj

    """
    def _try_execute(self, req, retry=3):

        while retry > 0:
            try:
                resp = req.execute()
                return resp
            except HttpError as e:
                if e.resp.status >= 500 and retry > 0:
                    retry -= 1
                    continue
                raise
    """

    def read_csv(self, uri, sep=",", header="infer"):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.get_media(bucket=bucket, object=file_path)
        resp = self._try_execute(req)
        df = pd.read_csv(BytesIO(resp), sep=sep, header=header)
        return df

    def write_csv(self, df, uri, sep=",", retry=3):

        bucket, file_path = self._parse_uri(uri)
        buf = BytesIO()
        df.to_csv(buf, index=False, sep=sep)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(buf.getvalue(), mimetype='text/csv'))
        resp = self._try_execute(req, retry=retry)

    def read_blob(self, uri, retry=3):

        bucket, file_path = self._parse_uri(uri)
        req = self._gsservice.objects().get_media(bucket=bucket, object=file_path)
        resp = self._try_execute(req, retry=retry)
        blob = pickle.loads(resp)
        return blob

    def write_blob(self, blob, uri, retry=3):

        bucket, file_path = self._parse_uri(uri)
        dump = pickle.dumps(blob)
        media_body = MediaInMemoryUpload(dump, mimetype='application/octet-stream')
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=media_body)
        resp = self._try_execute(req, retry=retry)

    def write_text(self, text, uri, retry=3):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(text, mimetype='text/plain'))
        resp = self._try_execute(req, retry=retry)

    def write_figure(self, figure, uri, image_type="png", retry=3):

        bucket, file_path = self._parse_uri(uri)
        buf = BytesIO()
        figure.savefig(buf, format=image_type)
        media_body = MediaInMemoryUpload(buf.getvalue(), mimetype='image/'+image_type)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket, name=file_path, media_body=media_body)
        resp = self._try_execute(req, retry=retry)

    def delete_object(self, uri, retry=3):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.delete(bucket=bucket, object=file_path)
        resp = self._try_execute(req, retry=retry)

    def get_bucket(self, bucket):

        buckets = self._gsservice.buckets()
        req = buckets.get(bucket=bucket)
        try:
            resp = self._try_execute(req)
        except HttpError as e:
            if e.resp.status == 404:
                return None
            raise

        return resp

    def list_bucket(self, projection=None, pageToken=None, prefix=None, maxResults=None):

        buckets = self._gsservice.buckets()
        req = buckets.list(project=self._project_id)
        resp = self._try_execute(req)
        return resp

    def create_bucket(self, bucket, location=None, storage_class=None, projection=None, retry=3):

        body = {
            "name": bucket
        }

        if location is not None:
            body["location"] = location

        buckets = self._gsservice.buckets()
        req = buckets.insert(project=self._project_id, body=body, projection=projection)
        resp = self._try_execute(req, retry=retry)
        return resp

    def delete_bucket(self, bucket, retry=3):

        buckets = self._gsservice.buckets()
        req = buckets.delete(bucket=bucket)
        resp = self._try_execute(req, retry=retry)
