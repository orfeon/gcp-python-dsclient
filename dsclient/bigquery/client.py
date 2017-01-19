from __future__ import print_function
import time
import pandas as pd
from io import BytesIO
from apiclient.http import MediaInMemoryUpload
from apiclient.http import BatchHttpRequest
from googleapiclient.errors import HttpError
from .. base import ClientBase
from .. schema import Schema, convert_df2bqschema
from .. errors import BigQueryError


class Client(ClientBase):

    __ENDPOINT_GBQ = "https://www.googleapis.com/auth/bigquery"
    __API_NAME = "bigquery"
    __API_VERSION = "v2"


    def __init__(self, project_id, keyfile_path=None, account_email=None):

        super(Client, self).__init__(project_id, keyfile_path, account_email)
        self._bqcredentials, self._bqservice = super(Client, self)._build_service(Client.__API_NAME,
                                                                                  Client.__API_VERSION,
                                                                                  Client.__ENDPOINT_GBQ)

    def _parse_table_name(self, table_name):

        if not "." in table_name:
            error_message = """
                table name({0}) must have dataset and table (dataset.table)
            """.format(table_name)
            raise Exception(error_message)

        dataset_id, table_id = table_name.split(".", 1)
        return dataset_id, table_id

    def _check_joberror(self, job):

        if "status" in job and "errorResult" in job["status"]:
            ereasons  = [error["reason"] for error in job["status"]["errors"]]
            emessages = [error["message"] for error in job["status"]["errors"]]
            reasons  = [job["status"]["errorResult"]["reason"]]
            messages = [job["status"]["errorResult"]["message"] + ", ".join([r + ": " + m for r, m in zip(ereasons, emessages)])]
            raise BigQueryError(reasons, messages)

    def _wait_job(self, job, jobname="JOB"):

        jobs = self._bqservice.jobs()
        job_id = job["jobReference"]["jobId"]
        state = job["status"]["state"]
        wait_second = 0
        while "DONE" != state:
            print("\r[{0}] {1} (waiting {2}s)".format(jobname, state, wait_second), end="")
            time.sleep(1)
            wait_second += 1
            req = jobs.get(projectId=self._project_id, jobId=job_id)
            resp = self._try_execute(req)
            state = resp["status"]["state"]
        print("\r[{0}] {1} (waited {2}s)\n".format(jobname, state, wait_second))
        self._check_joberror(job)

        return job

    def _try_execute_and_wait(self, req, jobname="BQ JOB", retry=3):

        while True:
            try:
                resp = self._try_execute(req)
                resp = self._wait_job(resp, jobname)
                return resp
            except BigQueryError as e:
                if 503 in e.codes:
                    print("BackendError. Trying again.")
                    retry -= 1
                    if retry <= 0:
                        raise
                    continue
                raise

    def _job2series(self, json_job):

        jtime = 9*60*60*1000

        jobid = json_job["jobReference"]["jobId"]
        state = json_job["status"]["state"]
        ctime = int(json_job["statistics"]["creationTime"]) + jtime
        stime = int(json_job["statistics"]["startTime"]) + jtime
        etime = int(json_job["statistics"].get("endTime", 0)) + jtime
        bsize = json_job["statistics"].get("query", {"query":{}}).get("totalBytesProcessed", 0)

        ctimes = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ctime/1000.0))
        stimes = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(stime/1000.0))
        etimes = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(etime/1000.0)) if etime != jtime else "-"
        #stimes = datetime.datetime.fromtimestamp(stime/1000.0)

        return pd.Series([jobid,state,ctimes,stimes,etimes,bsize],
                         index=["jobid","state","creationTime",
                                "startTime","endTime","bsize"])

    def query(self, query, table_name=None, append=True,
               write_disposition=None, allow_large_results=True, block=True,):

        if table_name is None:
            return self._query_and_get(query)

        return self._query_and_insert(query=query, table_name=table_name,append=append,
                                      write_disposition=write_disposition, allow_large_results=allow_large_results, block=block)

    def _query_and_get(self, query):

        jobs = self._bqservice.jobs()
        body={'query': query, 'timeoutMs': 200000}
        req = jobs.query(projectId=self._project_id,body=body)
        resp = self._try_execute(req)

        def _check_resperror(rsp):
            if "errors" in rsp:
                raise Exception(":".join([error["reason"] + error["message"] for error in rsp["errors"]]))

        start_sec = time.time()
        job_id = resp["jobReference"]["jobId"]
        retry_count = 100
        while retry_count > 0 and not resp["jobComplete"]:
            retry_count -= 1
            time.sleep(3)
            req = jobs.getQueryResults(projectId=self._project_id,
                                       jobId=job_id,
                                       timeoutMs=200000)
            resp = self._try_execute(req)
            _check_resperror(resp)

        schema = Schema(resp["schema"])

        df_list = []
        df = schema.to_dataframe(resp["rows"])
        df_list.append(df)

        current_row_size = len(df)
        while current_row_size < int(resp["totalRows"]):
            page_token = resp.get('pageToken', None)
            req = jobs.getQueryResults(projectId=self._project_id,
                                       jobId=job_id,
                                       pageToken=page_token,
                                       timeoutMs=100000)
            resp = self._try_execute(req)
            _check_resperror(resp)
            df = schema.to_dataframe(resp["rows"])
            df_list.append(df)
            current_row_size += len(df)
            current_sec = int(time.time() - start_sec)
            row_rate = int(100 * current_row_size / float(resp["totalRows"]))
            print("\r rows: read {0} / total {1} ({2}%), time: {3}s".format(current_row_size, resp["totalRows"], row_rate, current_sec), end="")

        dfs = pd.concat(df_list)
        return dfs

    def _query_and_insert(self, query, table_name=None, append=True, block=True,
               write_disposition=None, allow_large_results=True):

        dataset_id, table_id = self._parse_table_name(table_name)
        if write_disposition is None: #WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
            write_disposition = "WRITE_APPEND" if append else "WRITE_TRUNCATE"

        body = {"configuration": {
                  "query": {
                    "query": query,
                    "writeDisposition": write_disposition,
                    "allowLargeResults": allow_large_results,
                    "destinationTable": {
                      "projectId": self._project_id,
                      "datasetId": dataset_id,
                      "tableId": table_id
                    }
                  }
               }}

        jobs = self._bqservice.jobs()
        req = jobs.insert(projectId=self._project_id, body=body)

        if not block:
            resp = self._try_execute(req)
            self._check_joberror(resp)
            return resp["jobReference"]["jobId"]

        resp = self._try_execute_and_wait(req, "BQ INSERT")

        return resp

    def insert(self, df, table_name):

        dataset_id, table_id = self._parse_table_name(table_name)
        tabledata = self._bqservice.tabledata()

        for i in range(0, len(df), 5000):

            rows = [{"json": dict(record)} for ix, record in df[i:i+5000].iterrows()]
            req = tabledata.insertAll(projectId=self._project_id,
                                      datasetId=dataset_id,
                                      tableId=table_id,
                                      body={'rows': rows}).execute()
            resp = req.execute()
            time.sleep(0.05)

    def load(self, df, table_name, append=True, block=True, job_id=None,
             write_disposition=None, create_disposition="CREATE_IF_NEEDED"):
        """
        Upload local pandas.DataFrame to BigQuery table.

        Parameters
        ----------
        table_name : str
            dataset.table
        append : boolean
            if True, append records to existing table (if not exist, create new table).
            if False, if table exist, delete it and create new table. And append records.
        write_disposition : str
            WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY.
        create_disposition : str
            CREATE_NEVER, CREATE_IF_NEEDED

        Returns
        -------
        JSON job
            load job as JSON format.
        """

        dataset_id, table_id = self._parse_table_name(table_name)
        table = self.get_table(table_name)

        if write_disposition is None:
            write_disposition  = "WRITE_APPEND" if append else "WRITE_TRUNCATE"

        body = {"configuration": {
                  "load": {
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "createDisposition": create_disposition,
                    "writeDisposition": write_disposition,
                    "destinationTable": {
                      "projectId": self._project_id,
                      "datasetId": dataset_id,
                      "tableId": table_id
                    }
                  }
               }}

        if job_id is not None:
            body["jobReference"] = {"jobId": job_id, "projectId": self._project_id}

        if isinstance(df, str):
            df = pd.read_csv(df)

        if table is None or not append:
            schema = convert_df2bqschema(df)
            body["configuration"]["load"]["schema"] = schema

        buf = BytesIO()
        df.to_csv(buf, index=False)

        jobs = self._bqservice.jobs()
        req = jobs.insert(projectId=self._project_id,
                          body=body,
                          media_body=MediaInMemoryUpload(buf.getvalue(),
                                                         mimetype='application/octet-stream',
                                                         resumable=True))

        if not block:
            resp = self._try_execute(req)
            self._check_joberror(resp)
            return resp["jobReference"]["jobId"]

        resp = self._try_execute_and_wait(req, "BQ LOAD")

        return resp

    def extract(self, table_name, uri, block=True):

        dataset_id, table_id = self._parse_table_name(table_name)
        body = {"configuration": {
                    "extract": {
                        "sourceTable": {
                            "projectId": self._project_id,
                            "datasetId": dataset_id,
                            "tableId": table_id
                        },
                        "destinationUri": uri,
                        #"destinationUris": [string],
                        "printHeader": True
                        #"fieldDelimiter": string,
                        #"destinationFormat": string,
                        #"compression": string
                    }
               }}
        jobs = self._bqservice.jobs()
        req = jobs.insert(projectId=self._project_id, body=body)

        if not block:
            resp = self._try_execute(req)
            self._check_joberror(resp)
            return resp["jobReference"]["jobId"]

        resp = self._try_execute_and_wait(req, "BQ EXTRACT")

        table = self.get_table(table_name)
        return table

    def cancel(self, job_id):

        jobs = self._bqservice.jobs()
        req = jobs.cancel(projectId=self._project_id, jobId=job_id)
        resp = self._try_execute(req)
        return resp

    def get_dataset(self, dataset_id):

        datasets = self._bqservice.datasets()
        try:
            req = datasets.get(projectId=self._project_id, datasetId=dataset_id)
            resp = self._try_execute(req)
        except HttpError as e:
            if e.resp.status == 404:
                return None
            raise

        return resp

    def list_dataset(self, all=None, page_token=None, max_results=None):

        datasets = self._bqservice.datasets()
        try:
            req = datasets.list(projectId=self._project_id,
                                all=all,
                                pageToken=page_token,
                                maxResults=max_results)
            resp = self._try_execute(req)
        except HttpError as e:
            if e.resp.status == 404:
                return []
            raise

        return resp

    def create_dataset(self, dataset_id, location=None, expiration_ms=None):

        body = {
            "datasetReference": {
                "projectId": self._project_id,
                "datasetId": dataset_id
            }
        }

        if location is not None:
            body["location"] = location
        if expiration_ms is not None:
            body["defaultTableExpirationMs"] = expiration_ms

        datasets = self._bqservice.datasets()
        req = datasets.insert(projectId=self._project_id,
                              body=body)
        resp = self._try_execute(req)
        return resp

    def delete_dataset(self, dataset_id, delete_contents=False):

        datasets = self._bqservice.datasets()
        req = datasets.delete(projectId=self._project_id,
                              datasetId=dataset_id,
                              deleteContents=delete_contents)
        resp = self._try_execute(req)

    def get_table(self, table_name):

        dataset_id, table_id = self._parse_table_name(table_name)
        tables = self._bqservice.tables()
        try:
            req = tables.get(projectId=self._project_id,
                             datasetId=dataset_id,
                             tableId=table_id)
            resp = self._try_execute(req)
        except HttpError as e:
            if e.resp.status == 404:
                return None
            raise

        return resp

    def list_table(self, dataset_id, page_token=None, max_results=None):

        tables = self._bqservice.tables()
        req = tables.list(projectId=self._project_id,
                          datasetId=dataset_id,
                          pageToken=page_token,
                          maxResults=max_results)
        resp = self._try_execute(req)
        return resp

    def create_table(self, table_name, body):
        """
        Create table.

        Parameters
        ----------
        table_name : str
            dataset.table.
        body : str
            JSON format.

        Returns
        -------
        JSON table
            inserted table as JSON format.
        """

        dataset_id, table_id = self._parse_table_name(table_name)
        tables = self._bqservice.tables()
        req = tables.insert(projectId=self._project_id,
                            datasetId=dataset_id,
                            tableId=table_id,
                            body=body)
        resp = self._try_execute(req)
        return resp

    def delete_table(self, table_name):

        dataset_id, table_id = self._parse_table_name(table_name)
        tables = self._bqservice.tables()
        req = tables.delete(projectId=self._project_id,
                            datasetId=dataset_id,
                            tableId=table_id)
        resp = self._try_execute(req)

    def patch_table(self, table_name, body):

        dataset_id, table_id = self._parse_table_name(table_name)
        tables = self._bqservice.tables()
        req = tables.patch(projectId=self._project_id,
                           datasetId=dataset_id,
                           tableId=table_id,
                           body=body)
        resp = self._try_execute(req)
        return resp

    def show_jobs(self):

        jobs = self._bqservice.jobs()
        req = jobs.list(projectId=self._project_id)
        resp = self._try_execute(req)
        job_list = []
        for json_job in resp["jobs"]:
            job = self._job2series(json_job)
            job_list.append(job)
        print(pd.DataFrame(job_list))

    def show_job(self, job_id):

        jobs = self._bqservice.jobs()
        req = jobs.get(projectId=self._project_id, jobId=job_id)
        resp = self._try_execute(req)
        job = self._job2series(resp)
        print(resp)
        print(job)

    def _streaming_insert(self, df, dataset_id, table_id):

        tabledata = self._bqservice.tabledata()

        def insert():
            resp = tabledata.insertAll(projectId=self._project_id,
                                       datasetId=dataset_id,
                                       tableId=table_id,
                                       body=insert_data).execute()
            return resp

        for i in range(0, len(df), 5000):
            rows = []
            for ix, record in df[i:i+5000].iterrows():
                rows.append({"json": dict(record)})

            insert_data={'rows': rows}
            resp = insert()
            time.sleep(0.05)
