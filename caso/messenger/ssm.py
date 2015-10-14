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

import dirq.QueueSimple
from oslo_config import cfg
from oslo_log import log

import caso.messenger
from caso import utils

LOG = log.getLogger(__name__)

opts = [
    cfg.StrOpt('output_path',
               default='/var/spool/apel/outgoing/openstack',
               help="Directory to put the generated SSM records."),
]

CONF = cfg.CONF

CONF.register_opts(opts, group="ssm")


class SSMMessengerV02(caso.messenger.BaseMessenger):
    header = "APEL-cloud-message: v0.2"
    separator = "%%"

    def __init__(self):
        # FIXME(aloga): try except here
        utils.makedirs(CONF.ssm.output_path)

    def push(self, records):
        if not records:
            return

        entries = []
        for _, record in records.iteritems():
            aux = ""
            for k, v in record.as_dict().iteritems():
                if v is not None:
                    aux += "%s: %s\n" % (k, v)
            entries.append(aux)

        message = "%s\n" % self.header

        sep = "%s\n" % self.separator
        message += "%s" % sep.join(entries)

        # FIXME(aloga): try except here
        queue = dirq.QueueSimple.QueueSimple(CONF.ssm.output_path)
        queue.add(message)


class SsmMessager(SSMMessengerV02):
    def __init__(self):
        LOG.warning("Using deprecated caso.messenger.ssm.SsmMessager, "
                    "please use caso.messenger.ssm.SSMMessengerV02 if you "
                    "wish to continue usinf the 0.2 version of the record, "
                    "or refer to the cASO documentation.")

        super(SsmMessager, self).__init__()
