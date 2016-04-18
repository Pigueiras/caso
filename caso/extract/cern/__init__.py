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

from oslo_config import cfg

CONF = cfg.CONF
CONF.import_opt("site_name", "caso.extract.manager")
CONF.import_opt("user", "caso.extract.base", "extractor")
CONF.import_opt("password", "caso.extract.base", "extractor")
CONF.import_opt("endpoint", "caso.extract.base", "extractor")
CONF.import_opt("insecure", "caso.extract.base", "extractor")


import keystoneclient

from caso.extract.base import BaseExtractor


class V3BaseExtractor(BaseExtractor):
    def _get_keystone_session(self, tenant=None):
        auth = keystoneclient.auth.identity.v3.Password(
            username=CONF.extractor.user,
            password=CONF.extractor.password,
            auth_url=CONF.extractor.endpoint,
            user_domain_name='default',
            project_name=tenant,
            project_domain_name='default'
        )

        return keystoneclient.session.Session(auth=auth)

    def _get_keystone_client(self, tenant=None):
        """
        :param tenant: project ID.
        :rtype keystoneclient.httpclient.ZHTTPClient
        """
        return keystoneclient.v3.client.Client(session=self._get_keystone_session(tenant))

    def _get_keystone_users(self, ks_conn):
        """
        Gets the list of users for the default domain
        """
        users = ks_conn.users.list(domain="default")
        return {u.id: u.name for u in users}
