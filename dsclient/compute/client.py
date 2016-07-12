import time
import pandas as pd
from dsclient.client import ClientBase
from apiclient.http import BatchHttpRequest
from googleapiclient.errors import HttpError


class Client(ClientBase):

    __ENDPOINT_GCE = "https://www.googleapis.com/auth/compute"
    __API_NAME = "compute"
    __API_VERSION = "v1"

    def __init__(self, project_id, account_email=None, keyfile_path=None):

        super(Client, self).__init__(project_id, account_email, keyfile_path)
        self._ceservice = super(Client, self)._build_service(Client.__ENDPOINT_GCE,
                                                             Client.__API_NAME,
                                                             Client.__API_VERSION)

    def create_instance(self, name, zone, mtype, image):

        config = {
            'name': name,
            'machineType': "zones/" + zone + "/machineTypes/" + params["machine_type"],
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': "https://www.googleapis.com/compute/v1/projects/" + PROJECT_NAME + "/zones/" + zone + "/disks/" + params["image"]
                    }
                }
            ],
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [ENDPOINT_GCS, ENDPOINT_GBQ, ENDPOINT_GDS]
            }]
        }

        def callb(a,b,c):
            print a,b,c

        batch = BatchHttpRequest(callback=callb)

        instances = service.instances()
        for name in names:
            config = build_config(name, params)
            batch.add(instances.insert(project=self._project_id, zone=zone, body=config))
            print name

        a = batch.execute()


    def delete_instance(self, instance_id):

        pass

    def deploy_ipcluster(self, config):

        pass
