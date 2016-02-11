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

import dateutil.parser
from dateutil import tz
from oslo_config import cfg
from oslo_log import log
from oslo_utils import importutils
import six

SUPPORTED_EXTRACTORS = {
    'nova': 'caso.extract.nova.OpenStackExtractor',
    'ceilometer': 'caso.extract.ceilometer.CeilometerExtractor',
}

opts = [
    cfg.StrOpt('site_name',
               help='Site name as in GOCDB.'),
    cfg.StrOpt('service_name',
               default='$site_name',
               help='Service name within the site'),
    cfg.ListOpt('tenants',
                default=[],
                help='List of tenants to extract accounting records from.'),
]

cli_opts = [
    cfg.StrOpt('extract_from',
               help='Extract records from this date. If it is not set, '
               'extract records from last run. If none are set, extract '
               'records from the beginning of time. If no time zone is '
               'specified, UTC will be used.'),
    cfg.StrOpt('extractor',
               choices=SUPPORTED_EXTRACTORS,
               default='nova',
               help=('Which extractor to use for getting the data. '
                     'Only the following middlewares are supported: %s. '
                     'If you do not specify anything, nova will be '
                     'used.' % SUPPORTED_EXTRACTORS.keys())),
]

CONF = cfg.CONF

CONF.register_opts(opts)
CONF.register_cli_opts(cli_opts)

LOG = log.getLogger(__name__)


class Manager(object):
    def __init__(self):
        extractor_class = importutils.import_class(
            SUPPORTED_EXTRACTORS[CONF.extractor])
        self.extractor = extractor_class()
        self.records = None

    def _extract(self, extract_from):
        self.records = {}
        for tenant in CONF.tenants:
            try:
                records = self.extractor.extract_for_tenant(tenant,
                                                            extract_from)
            except Exception:
                records = []
                LOG.exception("Cannot extrat records for '%s'" % tenant)
            else:
                LOG.info("Extracted %d records for tenant '%s' from "
                         "%s to now" % (len(records), tenant, extract_from))
            self.records.update(records)

    def get_records(self, lastrun="1970-01-01"):
        """Get records from given date

        :param lastrun: date to get records from (optional).

        If CONF.extract_from is present, it will be used instead of the
        lastrun parameter.
        """
        extract_from = CONF.extract_from or lastrun

        if isinstance(extract_from, six.string_types):
            extract_from = dateutil.parser.parse(extract_from)

        if extract_from.tzinfo is None:
            extract_from.replace(tzinfo=tz.tzutc())
        if self.records is None:
            self._extract(extract_from)
        return self.records
