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
    __API_VERSION = "beta"

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

    @staticmethod
    def _check_exception(request_id, response, exception):

        if exception is not None:
            raise Exception(exception)

    @staticmethod
    def _clean_ipcontroller(profile):

        c0 = "'{print $2}'`"
        c1 = "kill -9 `ps -ef | grep 'ipcontroller start --profile {0}' | grep -v 'grep' |awk ".format(profile)
        ret = os.system(c1 + c0)
        ret = os.system("rm -f ~/.ipython/profile_{0}/pid/*".format(profile))
        ret = os.system("rm -f ~/.ipython/profile_{0}/security/*".format(profile))

    def get_current_instance_metadata(self, param):

        gh_url = 'http://metadata.google.internal/computeMetadata/v1/instance/{0}'.format(param)
        resp = requests.get(gh_url, headers={"Metadata-Flavor": "Google"})
        return resp.text

    def get_current_instance_name(self):

        name = self.get_current_instance_metadata("hostname").split(".")[0]
        return name

    def get_current_instance_zone(self):

        zone = self.get_current_instance_metadata("zone").split("/")[-1]
        return zone

    def get_current_instance_disk(self):

        disk = self.get_current_instance_metadata("disks/0/device-name")
        return disk

    def get_current_instance(self):

        name = self.get_current_instance_name()
        zone = self.get_current_instance_zone()
        resp = self.get_instance(zone=zone, name=name)
        return resp

    def stop_current_instance(self):

        name = self.get_current_instance_name()
        zone = self.get_current_instance_zone()
        self.stop_instance(zone, name)

    def create_current_snapshot(self, name):

        zone = self.get_current_instance_zone()
        disk = self.get_current_instance_disk()
        self.create_snapshot(name, zone, disk)

    def get_instance(self, zone, name):

        instances = self._ceservice.instances()
        req = instances.get(project=self._project_id, zone=zone, instance=name)
        resp = self._try_execute(req)
        return resp

    def list_instance(self, zone, filter_=None):

        instances = self._ceservice.instances()
        req = instances.list(project=self._project_id, zone=zone, filter=filter_)
        resp = self._try_execute(req)
        return resp

    def create_instance(self, zone, names,
                        mtype, disks=None, image=None, sizegb=10, preemptible=False,
                        network=None, external_ip=False,
                        metadata=None, tags=None, config={}):

        current_instance = self.get_current_instance()
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
                    "network": network
                }
            ],
            'scheduling': {
                'preemptible': preemptible
            },
            'tags': {
                'items': []
            },
            'serviceAccounts': [{
                'email': 'default',
                'scopes': ["https://www.googleapis.com/auth/cloud-platform"]
            }]
        }

        if external_ip:
            access_configs = [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}]
            init_config["networkInterfaces"][0]["accessConfigs"] = access_configs
        if tags is not None:
            if isinstance(tags, str):
                tags = [tags]
            init_config["tags"]["items"] = tags
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
        batch = self._ceservice.new_batch_http_request(callback=Client._check_exception)
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
                req = instances.insert(project=self._project_id, zone=zone, body=body)
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
            print("\r[CREATE INSTANCE] RUNNING: {0}, SUSPENDED: {1}, PROVISIONING: {2} (waiting {3}s)".format(ndone, nfailed, ndoing, wait_second), end="")
            time.sleep(1)
            wait_second += 1
        print("\r[CREATE INSTANCE] RUNNING: {0}, SUSPENDED: {1} (waited {2}s)\n".format(ndone, nfailed, wait_second), end="")

    def delete_instance(self, zone, names):

        if isinstance(names, str):
            names = [names]

        batch = self._ceservice.new_batch_http_request(callback=Client._check_exception)
        instances = self._ceservice.instances()
        for name in names:
            req = instances.delete(project=self._project_id,
                                   zone=zone, instance=name)
            batch.add(req)
        self._try_batch_execute(batch)

    def stop_instance(self, zone, names):

        if isinstance(names, str):
            names = [names]

        batch = self._ceservice.new_batch_http_request(callback=Client._check_exception)
        instances = self._ceservice.instances()
        for name in names:
            req = instances.stop(project=self._project_id,
                                 zone=zone,
                                 instance=name)
            batch.add(req)
        self._try_batch_execute(batch)

    def start_instance(self, zone, names):

        if isinstance(names, str):
            names = [names]

        batch = self._ceservice.new_batch_http_request(callback=Client._check_exception)
        instances = self._ceservice.instances()
        for name in names:
            req = instances.insert(project=self._project_id,
                                   zone=zone,
                                   instance=name)
            batch.add(req)
        self._try_batch_execute(batch)

    def create_disk(self, zone, names, snapshot=None, image=None):

        if snapshot is not None:
            if snapshot.startswith("global"):
                snapshot = "projects/{0}/{1}".format(self._project_id, snapshot)
            elif not snapshot.startswith("projects"):
                snapshot = "projects/{0}/global/snapshots/{1}".format(self._project_id, snapshot)
            config = {
                "sourceSnapshot": snapshot
            }
        elif image is not None:
            config = {
                "sourceImage": image
            }
        else:
            raise Exception("Both snapshot and image are None! Input at least one!")

        if isinstance(names, str):
            names = [names]

        def check_exception(request_id, response, exception):
            if exception is not None:
                raise Exception(exception)

        batch = self._ceservice.new_batch_http_request(callback=Client._check_exception)
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
            print("\r[CREATE DISK] DONE: {0}, FAILED: {1}, DOING: {2} (waiting {3}s)".format(ndone, nfailed, ndoing, wait_second), end="")
            time.sleep(1)
            wait_second += 1
        print("\r[CREATE DISK] DONE: {0}, FAILED: {1} (waited {2}s)\n".format(ndone, nfailed, wait_second), end="")

    def delete_disk(self, zone, disk):

        disks = self._ceservice.disks()
        req = disks.delete(project=self._project_id, zone=zone, disk=disk)
        resp = self._try_execute(req)

    def resize_disk(self, zone, disk, sizegb):

        config = {
            "sizeGb": sizegb
        }
        disks = self._ceservice.disks()
        req = disks.resize(project=self._project_id, zone=zone, disk=disk, body=config)
        resp = self._try_execute(req)

    def create_image(self, name, disk, snapshot=None):

        config = {
            "name": name,
            "rawDisk": {
                "source": "https://www.googleapis.com/compute/v1/projects/{0}/global/snapshots/{1}".format(self._project_id, snapshot_name)
            }
        }
        images = self._ceservice.images()
        req = images.insert(project=self._project_id, body=config)
        resp = self._try_execute(req)

        wait_second = 0
        status = resp["status"]
        while "DONE" != status:
            print("\r[CREATE IMAGE] {0} (waiting second: {1}s, {2}%)".format(status, wait_second, resp["progress"]), end="")
            time.sleep(5)
            wait_second += 5
            resp = images.get(project=self._project_id, image=name).execute()
            status = resp["status"]
        print("\r[CREATE IMAGE] DONE (waited second: {0}s)\n".format(wait_second))
        if 'error' in resp:
            raise Exception(resp['error'])

        return resp

    def delete_image(self, name):

        images = self._ceservice.images()
        req = images.delete(project=self._project_id, image=name)
        resp = self._try_execute(req)

    def create_snapshot(self, name, zone, disk):

        config = {
            "name": name
        }
        disks = self._ceservice.disks()
        req = disks.createSnapshot(project=self._project_id,
                                   zone=zone, disk=disk,
                                   body=config)
        resp = self._try_execute(req)

        snapshots = self._ceservice.snapshots()
        wait_second = 0
        status = resp["status"]
        while status not in ["DONE", "FAILED", "READY"]:
            print("\r[CREATE SNAPSHOT] {0} (waiting {1}s)".format(status, wait_second), end="")
            time.sleep(1)
            wait_second += 1
            resp = snapshots.get(project=self._project_id, snapshot=name).execute()
            status = resp["status"]
        print("\r[CREATE SNAPSHOT] {0} (waited {1}s)\n".format(status, wait_second), end="")
        if status == "FAILED":
            raise Exception("Failed to create snapshot from disk: {0}".format(disk))

        return resp

    def delete_snapshot(self, name):

        snapshots = service.snapshots()
        req = snapshots.delete(project=self._project_id, snapshot=name)
        resp = self._try_execute(req)

    def _check_params(self, zone, network, mtype, itype, core, pnum):

        current_instance = self.get_current_instance()
        if zone is None:
            zone = current_instance["zone"].split("/")[-1]
        if network is None:
            network = current_instance["networkInterfaces"][0]["network"]
        if mtype is None:
            if itype == "micro" or itype == "small":
                prefix = "f1" if itype == "micro" else "g1"
                mtype = "{0}-{1}".format(prefix, itype)
                core = 1
            elif itype in ["standard","highmem","highcpu"]:
                if core not in [1,2,4,8,16,32]:
                    raise Exception("core must be 1,2,4,8,16,32!")
                mtype = "n1-{0}-{1}".format(itype, core)
            else:
                raise Exception("itype must be standard,highmem,highcpu,small,micro!")
        pnum = core if pnum is None else pnum
        network_ip = current_instance["networkInterfaces"][0]["networkIP"]

        return zone, network, mtype, pnum, network_ip

    def _create_disks_from_snapshot(self, zone, names, snapshot):

        create_temp_snapshot = False
        if snapshot is None:
            disk = self.get_current_instance_disk()
            snapshot = "{0}-{1}-{2}".format(os.uname()[1], os.getpid(), int(time.time()))
            self.create_snapshot(snapshot, zone, disk)
            create_temp_snapshot = True

        self.create_disk(zone, names, snapshot=snapshot)
        if create_temp_snapshot:
            self.delete_snapshot(snapshot)

        return snapshot

    def _start_wait_ipcontroller(self, profile, network_ip, engine_file_path):

        command = "ipcontroller start --profile {0} --ip {1} &".format(profile, network_ip)
        ret = os.system(command)
        if ret != 0:
            raise Exception("Failed to start ipcontroller on this host!")

        retry = 30
        while not os.path.isfile(engine_file_path):
            time.sleep(1)
            retry -= 1
            if retry < 0:
                raise Exception("Failed to create engine file: {0}".format(engine_file_path))

        return engine_file_path

    def _create_startup_script(self, profile, engine_file_path, pnum):

        with open(engine_file_path, "r") as engine_file:
            engine_file_content = engine_file.read()

        startup_script = """
#! /bin/bash

ls -l /usr/local/bin
ipython profile create --parallel --profile={0}
rm -f ~/.ipython/profile_{0}/pid/*
rm -f ~/.ipython/profile_{0}/security/*

cat <<EOF > ~/.ipython/profile_{0}/security/ipcontroller-engine.json
{1}
EOF
ipcluster engines --profile {0} -n {2} --daemonize
        """.format(profile, engine_file_content, core if pnum is None else pnum)

        return startup_script

    def create_ipcluster(self, profile, itype="standard", core=1, num=1, pnum=None,
                         image=None, sizegb=10, snapshot=None, preemptible=False,
                         zone=None, network=None, mtype=None, external_ip=True, config=None):

        if snapshot is not None and image is not None:
            raise Exception("Both snapshot and image filled! chose one!")

        # check existing profile
        profile_dir = os.path.expanduser('~/.ipython/profile_{0}'.format(profile))
        if os.path.isdir(profile_dir):
            Client._clean_ipcontroller(profile)
        else:
            ret = os.system('ipython profile create --parallel --profile={0}'.format(profile))
            if ret != 0:
                raise Exception("Failed to create profile {0}".format(profile))

        zone, network, mtype, pnum, network_ip = self._check_params(zone, network, mtype, itype, core, pnum)

        names = ["ipcluster-{0}-{1}".format(profile, no) for no in range(num)]
        if image is None:
            snapshot = self._create_disks_from_snapshot(zone, names, snapshot)

        engine_file_path = profile_dir + "/security/ipcontroller-engine.json"
        self._start_wait_ipcontroller(profile, network_ip, engine_file_path)

        startup_script = self._create_startup_script(profile, engine_file_path, pnum)

        # create instances for ipengines.
        metadata = {"items": [{"key": "startup-script", "value": startup_script}]}
        if snapshot is not None:
            self.create_instance(zone=zone, names=names, mtype=mtype, disks=names,
                                 external_ip=external_ip, network=network, preemptible=preemptible,
                                 metadata=metadata)
        else:
            self.create_instance(zone=zone, names=names, mtype=mtype, image=image, sizegb=sizegb,
                                 external_ip=external_ip, network=network, preemptible=preemptible,
                                 metadata=metadata)

    def add_ipengine(self, profile, itype="standard", core=1, num=1, pnum=None,
                     image=None, sizegb=10, snapshot=None, preemptible=False,
                     mtype=None, external_ip=True, config=None):

        if snapshot is not None and image is not None:
            raise Exception("Both snapshot and image filled! chose one!")

        # check existing profile
        profile_dir = os.path.expanduser('~/.ipython/profile_{0}'.format(profile))
        if not os.path.isdir(profile_dir):
            raise Exception("No profile {0}".format(profile))

        zone, network, mtype, pnum, network_ip = self._check_params(None, None, mtype, itype, core, pnum)

        current_names = self.get_ipcluster_instance(profile)
        sindex = max([int(name.split("-")[-1]) for name in current_names]) + 1 if current_names else 0

        names = ["ipcluster-{0}-{1}".format(profile, no) for no in range(sindex,num+sindex)]
        if image is None:
            snapshot = self._create_disks_from_snapshot(zone, names, snapshot)

        engine_file_path = profile_dir + "/security/ipcontroller-engine.json"
        startup_script = self._create_startup_script(profile, engine_file_path, pnum)

        # create instances for ipengines.
        metadata = {"items": [{"key": "startup-script", "value": startup_script}]}
        if snapshot is not None:
            self.create_instance(zone=zone, names=names, mtype=mtype, disks=names,
                                 external_ip=external_ip, network=network, preemptible=preemptible,
                                 metadata=metadata)
        else:
            self.create_instance(zone=zone, names=names, mtype=mtype, image=image, sizegb=sizegb,
                                 external_ip=external_ip, network=network, preemptible=preemptible,
                                 metadata=metadata)

    def delete_ipcluster(self, profile):

        zone = self.get_current_instance_zone()
        names = self.get_ipcluster_instance(profile, zone)
        self.delete_instance(zone, names)
        Client._clean_ipcontroller(profile)

    def get_ipcluster_instance(self, profile, zone=None):

        if zone is None:
            zone = self.get_current_instance_zone()
        filter_str = "name eq ipcluster-{0}-[0-9]+".format(profile)
        instances = self.list_instance(zone, filter_str)
        if len(instances["items"]) == 0:
            return []
        names = [instance["name"] for instance in instances["items"]]
        return names
