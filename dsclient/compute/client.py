from __future__ import print_function
import os
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

    def __init__(self, project_id, keyfile_path=None, account_email=None):

        super(Client, self).__init__(project_id, keyfile_path, account_email)
        self._cecredentials, self._ceservice = super(Client, self)._build_service(Client.__API_NAME,
                                                                                  Client.__API_VERSION,
                                                                                  Client.__ENDPOINT_GCE)

    def _try_batch_execute(self, batch, retry=3):

        while retry > 0:
            try:
                batch.execute()
                return
            except TypeError:
                from httplib2 import Http
                self._cecredentials.refresh(Http())
                retry -= 1

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

    def stop_current_instance(self):
        current_instance = self.get_current_instance()
        name = current_instance["name"]
        zone = current_instance["zone"].split("/")[-1]
        self.stop_instance(name, zone)

    def get_instance(self, zone, name):
        req = self._ceservice.instances().get(project=self._project_id,
                                              zone=zone,
                                              instance=name)
        resp = self._try_execute(req)
        return resp

    def create_instance(self, names, mtype,
                        disks=None, image=None, sizegb=10,
                        zone=None, network=None,
                        preemptible=False, metadata=None, config={}):

        if zone is None or network is None:
            current_instance = self.get_current_instance()
            if zone is None:
                zone = current_instance["zone"].split("/")[-1]
            if network is None:
                network = current_instance["networkInterfaces"][0]["network"]

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
                    "network": network,
                    "accessConfigs": [{
                        "type": "ONE_TO_ONE_NAT",
                        "name": "External NAT"
                    }]
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

        if metadata is not None:
            init_config["metadata"] = metadata

        init_config.update(config)

        if image is None and disks is None:
            raise Exception("You must input image or disks!")

        if isinstance(names, str):
            names = [names]

        def check_exception(request_id, response, exception):
            if exception is not None:
                raise Exception(exception)

        #batch = BatchHttpRequest()
        batch = self._ceservice.new_batch_http_request(callback=check_exception)
        instances = self._ceservice.instances()

        if disks is not None:
            if isinstance(disks, str):
                disks = [disks]
            if len(names) != len(disks):
                raise Exception("instance num({0}) must be equal to disks num({1})!".format(len(names), len(disks)))
            for name, disk in zip(names, disks):
                body = init_config.copy()
                body.update({"name": name})
                body["disks"][0]["source"] = "zones/{0}/disks/{1}".format(zone, disk)
                req = instances.insert(project="stage-orfeon", zone=zone, body=body)
                batch.add(req)
        elif image is not None:
            init_config["disks"][0]["initializeParams"] = {'diskSizeGb': sizegb, 'sourceImage': image}
            for name in names:
                body = init_config.copy()
                body.update({"name": name})
                req = instances.insert(project=self._project_id, zone=zone, body=body)
                batch.add(req)

        self._try_batch_execute(batch)

        check_names  = list(names)
        failed_names = []
        nall = len(check_names)
        wait_second = 0
        while check_names:
            for check_name in check_names:
                resp = instances.get(project=self._project_id, zone=zone, instance=check_name).execute()
                if resp["status"] in ["RUNNING", "SUSPENDED", "TERMINATED"]:
                    check_names.remove(check_name)
                    if resp["status"] == "SUSPENDED":
                        failed_names.append(check_name)
            nfailed = len(failed_names)
            ndoing  = len(check_names)
            ndone   = nall - nfailed - ndoing
            print("\rRUNNING: {0}, SUSPENDED: {1}, PROVISIONING: {2} (waiting second: {3}s)".format(ndone, nfailed, ndoing, wait_second), end="")
            time.sleep(1)
            wait_second += 1
        print("\rRUNNING: {0}, SUSPENDED: {1} (waited second: {2}s)\n".format(ndone, nfailed, wait_second), end="")

    def delete_instance(self, zone, names, tag=None):

        if isinstance(names, str):
            names = [names]

        batch = BatchHttpRequest()
        instances = self._ceservice.instances()
        for name in names:
            req = instances.delete(project=self._project_id,
                                   zone=zone, instance=name)
            batch.add(req)
        self._try_batch_execute(batch)

        #req = instances.delete(project=self._project_id,
        #                       zone=zone,
        #                       instance=instance_id)
        #resp = req.execute()

    def stop_instance(self, names, zone=None):

        if zone is None:
            zone = self.get_instance_metadata("zone")

        if isinstance(names, str):
            names = [names]

        instances = self._ceservice.instances()
        if len(names) == 1:
            req = instances.stop(project=self._project_id,
                                 zone=zone,
                                 instance=names[0])
            resp = self._try_execute(req)
        elif len(names) > 1:
            for name in names:
                req = instances.stop(project=self._project_id,
                                     zone=zone,
                                     instance=name)
                batch.add(req)
            self._try_batch_execute(batch)
        else:
            raise Exception("instance name must not be vacant!")

    def start_instance(self, names=None, zone=None):

        if zone is None:
            zone = self.get_instance_metadata("zone")

        if isinstance(names, str):
            names = [names]

        instances = self._ceservice.instances()
        if len(names) == 1:
            req = instances.start(project=self._project_id,
                                  zone=zone,
                                  instance=names[0])
            resp = self._try_execute(req)
        elif len(names) > 1:
            for name in names:
                req = instances.insert(project=self._project_id,
                                       zone=zone,
                                       instance=name)
                batch.add(req)
            self._try_batch_execute(batch)
        else:
            raise Exception("instance name must not be vacant!")


    def create_disk(self, zone, disk, name, block=True):

        config = {
            "name": name,
            "sourceSnapshot": "global/snapshots/{0}".format(disk)
        }
        disks = self._ceservice.disks()
        req = disks.insert(project=self._project_id, zone=zone, body=config)
        resp = self._try_execute(req)
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

    def create_disks(self, names, snapshot, zone=None):

        if snapshot.startswith("global"):
            snapshot = "projects/{0}/{1}".format(self._project_id, snapshot)
        elif not snapshot.startswith("projects"):
            snapshot = "projects/{0}/global/snapshots/{1}".format(self._project_id, snapshot)

        if zone is None:
            zone = self.get_instance_metadata("zone")

        config = {
            "sourceSnapshot": snapshot
        }

        if isinstance(names, str):
            names = [names]

        def check_exception(request_id, response, exception):
            if exception is not None:
                raise Exception(exception)

        batch = self._ceservice.new_batch_http_request(callback=check_exception)
        disks = self._ceservice.disks()
        for name in names:
            body = config.copy()
            body.update({"name": name})
            job = disks.insert(project=self._project_id, zone=zone, body=body)
            batch.add(job)
        self._try_batch_execute(batch)

        check_names = list(names)
        nall = len(check_names)
        failed_names = []
        wait_second = 0
        while check_names:
            for check_name in check_names:
                resp = disks.get(project=self._project_id, zone=zone, disk=check_name).execute()
                if resp["status"] in ["DONE","FAILED","READY"]:
                    check_names.remove(check_name)
                    if resp["status"] == "FAILED":
                        failed_names.append(check_name)
            nfailed = len(failed_names)
            ndoing  = len(check_names)
            ndone   = nall - nfailed - ndoing
            print("\rDONE: {0}, FAILED: {1}, DOING: {2} (waiting second: {3}s)".format(ndone, nfailed, ndoing, wait_second), end="")
            time.sleep(1)
            wait_second += 1
        print("\rDONE: {0}, FAILED: {1} (waited second: {2}s)\n".format(ndone, nfailed, wait_second), end="")

    def delete_disk(self, zone, disk):

        disks = self._ceservice.disks()
        req = disks.delete(project=self._project_id, zone=zone, disk=disk)
        resp = self._try_execute(req)

    def resize_disk(self, zone, disk, sizeGb):

        config = {
            "sizeGb": sizeGb
        }
        disks = self._ceservice.disks()
        req = disks.resize(project=self._project_id, zone=zone, disk=disk, body=config)
        resp = self._try_execute(req)

    def create_image(self, disk, image_name, block=True):

        config = {
            "name": image_name,
            "rawDisk": {
                "source": "https://www.googleapis.com/compute/v1/projects/{0}/global/snapshots/{1}".format(self._project_id, snapshot_name)
            }
        }
        images = self._ceservice.images()
        req = images.insert(project=self._project_id, body=config)
        resp = self._try_execute(req)

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
        resp = self._try_execute(req)

    def create_snapshot(self, zone, disk, snapshot_name, block=True):

        config = {
            "name": snapshot_name
        }
        disks = self._ceservice.disks()
        req = disks.createSnapshot(project=self._project_id,
                                   zone=zone, disk=disk,
                                   body=config)
        resp = self._try_execute(req)

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
        resp = self._try_execute(req)

    def deploy_ipcluster(self, profile, itype="standard", core=1, num=1, icore=None,
                         image=None, sizegb=10, snapshot=None, preemptible=False,
                         zone=None, network=None, mtype=None, config=None):

        # create disks for instances.
        current_instance = self.get_current_instance()
        if zone is None:
            zone = current_instance["zone"].split("/")[-1]
        if network is None:
            network = current_instance["networkInterfaces"][0]["network"]
        if mtype is None:
            if itype == "micro" or itype == "small":
                prefix = "f1" if itype == "micro" else "g1"
                mtype = "{0}-{1}".format(prefix, itype)
            else:
                mtype = "n1-{0}-{1}".format(itype, core)

        # create disks from snapshot.
        if snapshot is None and image is None:
            disk = self.get_instance_metadata("disks/0/device-name")
            snapshot = "{0}-{1}-{2}".format(os.uname()[1], os.getpid(), int(time.time()))
            self.create_snapshot(zone, disk, snapshot)

        names = ["ipcluster-{0}-{1}".format(profile, no) for no in range(num)]
        if snapshot is not None:
            disks = names
            self.create_disks(disks, snapshot, zone=zone)

        # start ipcontroller on current instance.
        profile_dir = os.path.expanduser('~/.ipython/profile_{0}'.format(profile))
        if not os.path.isdir(profile_dir):
            command = 'ipython profile create --parallel --profile={0}'.format(profile)
            ret = os.system(command)
            if ret != 0:
                raise Exception("")

        network_ip = current_instance["networkInterfaces"][0]["networkIP"]
        command = "ipcontroller start --profile {0} --ip {1} &".format(profile, network_ip)
        ret = os.system(command)
        if ret != 0:
            raise Exception("")

        retry = 30
        engine_file_path = profile_dir + "/security/ipcontroller-engine.json"
        while not os.path.isfile(engine_file_path):
            time.sleep(1)
            retry -= 1
            if retry < 0:
                raise Exception("No config file: {0}".format(engine_file_path))

        # create ipcontroller-engine.json for ipengine hosts.
        with open(engine_file_path, "r") as engine_file:
            engine_file_content = engine_file.read()

        startup_script = """
#! /bin/bash

mkdir -p ~/.ipython/profile_{0}/security
cat <<EOF > ~/.ipython/profile_{0}/security/ipcontroller-engine.json
{1}
EOF
ipcluster engines --profile {0} -n {2}
        """.format(profile, engine_file_content, core if icore is None else icore)

        # create instances for ipengines.
        metadata = {"items": [{"key": "startup-script", "value": startup_script}]}
        if snapshot is not None:
            self.create_instance(names=names, mtype=mtype, disks=disks,
                                 zone=zone, network=network, metadata=metadata, preemptible=preemptible)
        else:
            self.create_instance(names=names, mtype=mtype, image=image, sizegb=sizegb,
                                 zone=zone, network=network, metadata=metadata, preemptible=preemptible)
