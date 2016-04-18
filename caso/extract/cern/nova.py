# -*- coding: utf-8 -*-

# Copyright 2014 Spanish National Research Council (CSIC)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import datetime
import operator

import dateutil.parser
import novaclient.client
from dateutil import tz
from caso import record

from caso.extract.cern import V3BaseExtractor, CONF


class NovaExtractor(V3BaseExtractor):
    def __init__(self):
        super(NovaExtractor, self).__init__()
        self.users = None

    def _get_servers(self, nova_client, since):
        """
        Fetch a list of servers.
        :type nova_client: novaclient
        :type since: datetime.datetime
        """
        # FIXME (lpigueir): Horrible implementation
        servers = nova_client.servers.list()
        state_changed = nova_client.servers.list(search_opts={"changes-since": since})
        servers = set(servers).union(state_changed)

        servers = sorted([s for s in servers], key=operator.attrgetter("created"))

        return sorted(servers, key=operator.attrgetter("created"))

    def _generate_base_cloud_record(self, server, images, users, vo):
        """
        Generate a CloudRecord based on information fetched from nova
        """
        status = self.vm_status(server.status)
        image_id = None

        for image in images:
            if image.id == server.image['id']:
                image_id = image.metadata.get("vmcatcher_event_ad_mpuri",
                                              None)
                break

        if image_id is None:
            image_id = server.image['id']

        return record.CloudRecord(server.id,
                                  CONF.site_name,
                                  server.name,
                                  server.user_id,
                                  server.tenant_id,
                                  vo,
                                  cloud_type="OpenStack",
                                  status=status,
                                  image_id=image_id,
                                  user_dn=users.get(server.user_id, None))

    def _get_conn(self, tenant):
        client = novaclient.client.Client
        conn = client(
                2,
                session=self._get_keystone_session(tenant),
                insecure=CONF.extractor.insecure)
        return conn

    def _get_users(self, ks_client):
        if self.users is None:
            self.users = self._get_keystone_users(ks_client)

        return self.users

    def extract_for_tenant(self, tenant, _from):
        """Extract records for a tenant from given date querying nova.

        This method will get information from nova.

        :param tenant: Tenant to extract records for.
        :param _from: datetime.datetime object indicating the date to
                             extract records from

        :returns: A dictionary of {"server_id": caso.record.Record"}
        """
        # Some API calls do not expect a TZ, so we have to remove the timezone
        # from the dates. We assume that all dates coming from upstream are
        # in UTC TZ.

        now = datetime.datetime.now(tz.tzutc()).replace(tzinfo=None)
        end = now + datetime.timedelta(days=1)

        # Try and except here
        nova_client = self._get_conn(tenant)
        ks_client = self._get_keystone_client(tenant)
        users = self._get_users(ks_client)
        tenant_id = ks_client.session.get_project_id()
        servers = self._get_servers(nova_client, since=_from)

        if servers:
            start = dateutil.parser.parse(servers[0].created)
            start = start.replace(tzinfo=None)
        else:
            start = lastrun

        aux = nova_client.usage.get(tenant_id, start, end)
        usages = getattr(aux, "server_usages", [])

        images = nova_client.images.list()
        records = {}

        vo = self.voms_map.get(tenant)

        for server in servers:
            records[server.id] = self._generate_base_cloud_record(server,
                                                                  images,
                                                                  users,
                                                                  vo)

        for usage in usages:
            if usage["instance_id"] not in records:
                continue
            instance_id = usage["instance_id"]
            records[instance_id].memory = usage["memory_mb"]
            records[instance_id].cpu_count = usage["vcpus"]
            records[instance_id].disk = usage["local_gb"]

            started = dateutil.parser.parse(usage["started_at"])

            records[instance_id].start_time = int(started.strftime("%s"))
            if usage.get("ended_at", None) is not None:
                ended = dateutil.parser.parse(usage['ended_at'])
                records[instance_id].end_time = int(ended.strftime("%s"))
                wall = ended - started
            else:
                wall = now - started

            wall = int(wall.total_seconds())
            records[instance_id].wall_duration = wall
            records[instance_id].cpu_duration = wall

        return records
