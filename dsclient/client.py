import httplib2
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build


class ClientBase(object):

    def __init__(self, project_id=None, account_email=None, keyfile_path=None):

        self._project_id    = project_id
        self._account_email = account_email
        self._keyfile_path  = keyfile_path

    def _build_service(self, scope, api_name, api_version):

        if self._account_email is None or self._keyfile_path is None:
            credentials = GoogleCredentials.get_application_default()
            return build(api_name, api_version, credentials=credentials)
        else:
            credentials = ServiceAccountCredentials.from_p12_keyfile(
                self._account_email,
                self._keyfile_path,
                scopes=scope)

            http = httplib2.Http()
            auth_http = credentials.authorize(http)
            service = build(api_name, api_version, http=auth_http)
            return service
