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
import json

from oslo_config import cfg
from oslo_log import log

import caso.messenger


opts = [
    cfg.StrOpt('out',
               default="out.json",
               help='File to output JSON-formatted records to.')
]


CONF = cfg.CONF
CONF.register_opts(opts, group="json")

LOG = log.getLogger(__name__)


class JsonMessenger(caso.messenger.BaseMessenger):
    """Format and send records to a file. Each line will be a
    JSON record to be processed"""

    def __init__(self):
        super(JsonMessenger, self).__init__()
        self.path = CONF.json.out

    def push(self, records):
        with open(self.path, 'w') as target:
            target.truncate()
            for _, record in records.iteritems():
                target.write(json.dumps(record.as_dict()) + "\n")

        LOG.info("Saved %d records to %s." %
                 (len(records), self.path))
