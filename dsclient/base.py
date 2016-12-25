import httplib2
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from googleapiclient.errors import HttpError


class ClientBase(object):

    def __init__(self, project_id=None, keyfile_path=None, account_email=None):

        self._project_id    = project_id
        self._keyfile_path  = keyfile_path
        self._account_email = account_email

    def _build_service(self, api_name, api_version, scopes):

        if self._keyfile_path is None:
            credentials = GoogleCredentials.get_application_default()
            service = build(api_name, api_version, credentials=credentials)
            return credentials, service
        else:
            if self._keyfile_path.lower().endswith(".json"):
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    self._keyfile_path,
                    scopes=scopes)
            elif self._keyfile_path.lower().endswith(".p12"):
                if self._account_email is None:
                    raise Exception("Input account email.")
                credentials = ServiceAccountCredentials.from_p12_keyfile(
                    self._account_email,
                    self._keyfile_path,
                    scopes=scopes)
            else:
                error_message = """
                    Key file format [{0}] is illegal.
                    Key file must be .json or .p12.
                """.format(self._keyfile_path)
                raise Exception(error_message)

            #http = httplib2.Http()
            #auth_http = credentials.authorize(http)
            #service = build(api_name, api_version, http=auth_http)
            service = build(api_name, api_version, credentials=credentials)
            return credentials, service

    def _try_execute(self, req, retry=3):

        while True:
            try:
                resp = req.execute()
                return resp
            except HttpError as e:
                if e.resp.status >= 500:
                    retry -= 1
                    if retry <= 0:
                        raise
                    continue
                raise
            except:
                retry -= 1
                if retry <= 0:
                    raise
