import time
import json
import platform
import pickle
import pandas as pd
from io import BytesIO
from io import StringIO
from googleapiclient.http import MediaInMemoryUpload
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

    def get_gsservice(self):

        return self._gsservice

    def _parse_uri(self, uri):

        if not uri.startswith("gs://"):
            raise Exception("uri({0}) must start with 'gs://'".format(uri))

        path = uri.replace("gs://", "")
        bucket, obj = path.split("/", 1)
        return bucket, obj

    def _read(self, uri, retry=3):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.get_media(bucket=bucket, object=file_path)
        resp = self._try_execute(req, retry=retry)
        return resp

    def _write(self, obj, uri, mimetype=None, retry=3):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.insert(bucket=bucket,
                             name=file_path,
                             media_body=MediaInMemoryUpload(obj, mimetype=mimetype))
        resp = self._try_execute(req, retry=retry)
        return resp

    def read_csv(self, uri, sep=",", header="infer", dtype=None, retry=3):

        resp = self._read(uri=uri, retry=retry)
        df = pd.read_csv(BytesIO(resp), sep=sep, header=header, dtype=dtype)
        return df

    def write_csv(self, df, uri, sep=",", retry=3):

        if platform.python_version_tuple()[0] == "3":
            buf = StringIO()
            df.to_csv(buf, index=False)
            value = str.encode(buf.getvalue())
        else:
            buf = BytesIO()
            df.to_csv(buf, index=False, sep=sep)
            value = buf.getvalue()
        resp = self._write(obj=value, uri=uri, mimetype='text/csv', retry=retry)
        return resp

    def read_blob(self, uri, retry=3):

        resp = self._read(uri=uri, retry=retry)
        blob = pickle.loads(resp)
        return blob

    def write_blob(self, blob, uri, retry=3):

        dump = pickle.dumps(blob)
        resp = self._write(obj=dump, uri=uri, mimetype='application/octet-stream', retry=retry)
        return resp

    def read_text(self, uri, retry=3):

        resp = self._read(uri=uri, retry=retry)
        if platform.python_version_tuple()[0] == "3":
            resp = resp.decode('utf-8')
        return resp

    def write_text(self, text, uri, retry=3):

        if platform.python_version_tuple()[0] == "3":
            text = str.encode(text)
        resp = self._write(obj=text, uri=uri, mimetype='text/plain', retry=retry)
        return resp

    def read_json(self, uri, retry=3):

        resp = self._read(uri=uri, retry=retry)
        if platform.python_version_tuple()[0] == "3":
            s = resp.decode('utf-8')
            dic = json.load(StringIO(s))
        else:
            dic = json.load(BytesIO(resp))
        return dic

    def write_json(self, dic, uri, retry=3):

        if platform.python_version_tuple()[0] == "3":
            text = str.encode(json.dumps(dic))
        else:
            text = json.dumps(dic)
        resp = self._write(obj=text, uri=uri, mimetype='application/json', retry=retry)
        return resp

    def write_figure(self, figure, uri, image_type="png", retry=3):

        buf = BytesIO()
        figure.savefig(buf, format=image_type)
        resp = self._write(obj=buf.getvalue(), uri=uri, mimetype='image/'+image_type, retry=retry)
        return resp

    def delete_object(self, uri, retry=3):

        bucket, file_path = self._parse_uri(uri)
        objects = self._gsservice.objects()
        req = objects.delete(bucket=bucket, object=file_path)
        resp = self._try_execute(req, retry=retry)
        return resp

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
