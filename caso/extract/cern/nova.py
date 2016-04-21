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
from novaclient import client
from dateutil import tz
from caso import record

from caso.extract.cern import V3BaseExtractor, CONF


class NovaExtractor(V3BaseExtractor):
    def _get_novaclient(self, tenant):
        """TODO"""
        return client.Client(2, session=self._get_keystone_session(tenant),
                             insecure=CONF.extractor.insecure)

    def _join_servers_and_usage(self, usages, changes):
        # Complete the usages information
        servers_usage = []

        for change in changes:
            if change.id in usages:
                complete = usages[change.id]
                complete['user_id'] = change.user_id
                complete['image_id'] = change.image['id']
                servers_usage.append(complete)

        return servers_usage


    def _get_server_usage(self, nova_client, tenant_id, start, end):
        servers = nova_client.usage.get(tenant_id, start, end).server_usages

        result = {}

        for server in servers:
            result[server['instance_id']] = server

        return result


    def _get_changed_servers(self, nova_client, tenant, since):
        return nova_client.servers.list(
            search_opts={"changes-since": since})

    def _get_servers(self, tenant, _from, _to):
        nova_client = self._get_novaclient(tenant)
        keystone_client = self._get_keystone_client(tenant)

        tenant_id = keystone_client.session.get_project_id()

        servers_usage = self._get_server_usage(nova_client, tenant_id, _from,
                                               _to)
        if servers_usage:
            # TODO: Clean this a bit
            min_start = min(server['started_at'] for _, server in servers_usage.iteritems())
            min_start = dateutil.parser.parse(min_start)

            servers_changed = self._get_changed_servers(nova_client, tenant,
                                                        min_start)

            return self._join_servers_and_usage(servers_usage,
                                                       servers_changed)
        else:
            return []

    def _calculate_wall_duration(self, server):
        seconds_in_an_hour = 3600
        return server['hours'] * seconds_in_an_hour

    def _to_unix_timestamp(self, timestamp):
        return int(dateutil.parser.parse(timestamp).strftime("%s"))

    def _generate_record(self, server, vo):
        wall_duration = self._calculate_wall_duration(server)
        start_time = self._to_unix_timestamp(server['started_at'])
        if server['ended_at'] is not None:
            end_time = self._to_unix_timestamp(server['ended_at'])
        else:
            end_time = None

        return record.CloudRecord(
            server['instance_id'],
            CONF.site_name,
            server['name'],
            server['user_id'],
            server['tenant_id'],
            vo,
            cloud_type="OpenStack",
            status=server['state'],
            image_id=server['image_id'],
            user_dn=server['user_id'],
            wall_duration=wall_duration,
            cpu_count=server['vcpus'],
            memory=server['memory_mb'],
            disk=server['local_gb'],
            start_time=start_time,
            end_time=end_time)


    def _generate_records(self, servers, vo):
        records = {}
        for server in servers:
            records[server['instance_id']] = self._generate_record(server, vo)

        return records

    def extract_for_tenant(self, tenant, _from, _to):
        """Extract records for a tenant from given date querying nova.

        This method will get information from nova.

        :param tenant: Tenant to extract records for.
        :param _from: datetime.datetime object indicating the date to
                             extract records from
        :param to: datetime.datetime object indicating the date to
                   extract the records to

        :returns: A dictionary of {"server_id": caso.record.Record"}
        """
        vo = self.voms_map.get(tenant)
        servers = self._get_servers(tenant, _from, _to)
        records = self._generate_records(servers, vo)

        return records
