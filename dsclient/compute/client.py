from __future__ import print_function
import sys
import time
import requests
import pandas as pd
from apiclient.http import BatchHttpRequest
from googleapiclient.errors import HttpError
from .. base import ClientBase


class Client(ClientBase):

    __ENDPOINT_GCE = "https://www.googleapis.com/auth/compute"
    __API_NAME = "compute"
    __API_VERSION = "v1"

    __API_URI = "https://www.googleapis.com/{0}/{1}/".format(__API_NAME, __API_VERSION)

    def __init__(self, project_id, account_email=None, keyfile_path=None):

        super(Client, self).__init__(project_id, account_email, keyfile_path)
        self._ceservice = super(Client, self)._build_service(Client.__ENDPOINT_GCE,
                                                             Client.__API_NAME,
                                                             Client.__API_VERSION)

    def get_instance_metadata(self, param):

        gh_url = 'http://metadata.google.internal/computeMetadata/{0}/instance/{1}'.format(Client.__API_VERSION, param)
        resp = requests.get(gh_url, headers={"Metadata-Flavor": "Google"})
        return resp.text

    def get_current_instance(self):

        #instance_id = self.get_instance_metadata("id")
        hostname    = self.get_instance_metadata("hostname")
        zone        = self.get_instance_metadata("zone")
        #tags        = self.get_instance_metadata("tags")
        #return {"id": instance_id, "hostname": hostname, "zone": zone, "tags": tags}

        zone = zone.split("/")[-1]
        instance_name = hostname.split(".")[0]

        resp = self.get_instance(zone=zone, name=instance_name)
        return resp

    def get_instance(self, zone, name):
        req = self._ceservice.instances().get(project=self._project_id,
                                              zone=zone,
                                              instance=name)
        resp = req.execute()
        return resp

    def create_instance(self, names, zone, mtype,
                        simage=None, sdisk=None, disksizegb=10, network=None,
                        preemptible=False,
                        block=True, config={}):

        init_config = {
            'machineType': "zones/{0}/machineTypes/{1}".format(zone, mtype),
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True
                }
            ],
            'networkInterfaces': [
                {
                    "network": network
                }
            ],
            'scheduling': {
                'preemptible': preemptible
            },
            'serviceAccounts': [{
                'email': 'default',
                'scopes': ["https://www.googleapis.com/auth/cloud-platform"]
            }]
        }

        if simage is None and sdisk is None:
            raise Exception("You must input simage or sdisk!")

        if sdisk is not None:
            init_config["disks"][0]["source"] = sdisk
        elif simage is not None:
            init_config["disks"][0]["initializeParams"] = {'diskSizeGb': disksizegb, 'sourceImage': simage}

        init_config.update(config)

        def callb(a=None):
            print(a)

        batch = BatchHttpRequest(callback=callb)

        if isinstance(names, str):
            names = [names]

        instances = self._ceservice.instances()
        for name in names:
            body = init_config.copy()
            body.update({"name": name})
            body["disks"][0]["source"] = "zones/{0}/disks/{1}".format(zone, name),
            batch.add(instances.insert(project=self._project_id, zone=zone, body=body))

        #import httplib2
        #http = httplib2.Http()
        #auth_http = self._credentials.authorize(http)
        #resp = batch.execute(http=auth_http)
        resp = instances.insert(project=self._project_id, zone=zone, body=body).execute()
        print(resp)

        if not block:
            return resp

        return resp


    def delete_instance(self, zone, names, tag=None):

        if isinstance(names, str):
            names = [names]

        instances = self._ceservice.instances()
        for name in names:
            batch.add(instances.insert(project=self._project_id, zone=zone, instance=instance_id))
        resp = batch.execute()

        #req = instances.delete(project=self._project_id,
        #                       zone=zone,
        #                       instance=instance_id)
        #resp = req.execute()

    def stop_instance(self, names=None):
        pass

    def start_instance(self, names=None):
        pass

    def create_disk(self, zone, sdisk, name, block=True):

        config = {
            "name": name,
            "sourceSnapshot": "global/snapshots/{0}".format(sdisk)
        }
        disks = self._ceservice.disks()
        req = disks.insert(project=self._project_id, zone=zone, body=config)
        resp = req.execute()
        if 'error' in resp:
            raise Exception(resp['error'])

        if not block:
            return resp

        wait_second = 0
        status = resp["status"]
        while status not in ["DONE","FAILED","READY"]:
            print("\r{0} (waiting second: {1}s)".format(status, wait_second), end="")
            time.sleep(1)
            wait_second += 1
            resp = disks.get(project=self._project_id, zone=zone, disk=name).execute()
            status = resp["status"]
        print("\r{0} (waited second: {1}s)\n".format(status, wait_second), end="")
        if status == "FAILED":
            raise Exception()

        return resp

    def create_disks(self, zone, snapshot, names, block=True):

        config = {
            "sourceSnapshot": "global/snapshots/{0}".format(snapshot)
        }

        if isinstance(names, str):
            names = [names]

        #batch = BatchHttpRequest()
        self._ceservice = super(Client, self)._build_service(Client.__ENDPOINT_GCE,
                                                             Client.__API_NAME,
                                                             Client.__API_VERSION)
        batch = self._ceservice.new_batch_http_request()
        disks = self._ceservice.disks()
        for name in names:
            body = config.copy()
            body.update({"name": name})
            job = disks.insert(project=self._project_id, zone=zone, body=config)
            batch.add(job)
        resp = batch.execute()
        print(resp)

        if not block:
            return resp

        check_names = names.copy()
        nall = len(check_names)
        failed_names = []
        wait_second = 0
        while not check_names:
            for check_name in check_names:
                resp = disks.get(project=self._project_id, zone=zone, disk=check_name).execute()
                if resp["status"] in ["DONE","FAILED","READY"]:
                    check_names.remove(check_name)
                    if resp["status"] == "FAILED":
                        failed_names.append(check_name)
            nfailed = len(failed_names)
            ndoing  = len(check_names)
            ndone   = nall - nfailed - ndoing
            print("\rDONE: {0}, FAILED: {1}, DOING: {2}".format(ndone, nfailed, ndoing), end="")
            time.sleep(1)

    def delete_disk(self):

        config = {
            "sourceSnapshot": "projects/{0}/global/snapshots/{1}".format(self._project_id, name)
        }
        disks = self._ceservice.disks()
        req = disks.insert(project=self._project_id, body=config)

    def resize_disk(self, zone, disk, sizeGb):

        config = {
            "sizeGb": sizeGb
        }
        disks = self._ceservice.disks()
        req = disks.resize(project=self._project_id, zone=zone, disk=disk, body=config)
        resp = req.execute()

    def create_image(self, disk, image_name, block=True):

        config = {
            "name": image_name,
            "rawDisk": {
                "source": "https://www.googleapis.com/compute/v1/projects/stage-orfeon/global/snapshots/myjupyter4"
            }
        }
        images = self._ceservice.images()
        req = images.insert(project=self._project_id, body=config)
        resp = req.execute()

        if not block:
            return resp

        wait_second = 0
        status = resp["status"]
        while "DONE" != status:
            print("\r{0} (waiting second: {1}s, {2}%)".format(status, wait_second, resp["progress"]), end="")
            time.sleep(5)
            wait_second += 5
            resp = images.get(project=self._project_id, image=image_name).execute()
            status = resp["status"]
        print("\rDONE (waited second: {0}s)\n".format(wait_second))
        if 'error' in resp:
            raise Exception(resp['error'])

        return resp

    def delete_image(self, image_name, block=True):

        images = self._ceservice.images()
        req = images.delete(project=self._project_id, image=image_name)
        resp = req.execute()

    def create_snapshot(self, zone, disk, snapshot_name, block=True):

        config = {
            "name": snapshot_name
        }
        disks = self._ceservice.disks()
        req = disks.createSnapshot(project=self._project_id,
                                   zone=zone, disk=disk,
                                   body=config)
        resp = req.execute()

        if not block:
            return resp

        snapshots = self._ceservice.snapshots()
        wait_second = 0
        status = resp["status"]
        while status not in ["DONE", "FAILED", "READY"]:
            print("\r{0} (waiting second: {1}s)".format(status, wait_second), end="")
            time.sleep(1)
            wait_second += 1
            resp = snapshots.get(project=self._project_id, snapshot=snapshot_name).execute()
            status = resp["status"]
        print("\r{0} (waited second: {1}s)\n".format(status, wait_second), end="")
        if status == "FAILED":
            raise Exception()

        return resp

    def delete_snapshot(self, snapshot_name):
        req = service.snapshots().delete(project=self._project_id,
                                         snapshot=snapshot_name)
        resp = req.execute()

    def deploy_ipcluster(self, name, core=1, itype="normal", num=1,
                         icore=None, preemptible=False, disksize=None,
                         image=None, zone=None, mtype=None, config=None):

        if image is None:
            zone    = self.get_instance_metadata("zone")
            disk    = self.get_instance_metadata("disks/0/device-name")
            network = self.get_instance_metadata("network-interfaces/0/network")

            snapshot = self.create_snapshot(zone, disk, name)

        current_instance = self.get_current_instance()
        if icore is None:
            icore = core
        if image is None:
            image = current_instance[""]
