# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving host details from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from typing import List, Dict
import mysql.connector
from wwdtm.host import core, info, utility

#region Retrieval Functions
def retrieve_by_id(host_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with host details based on the requested
    host ID

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the host ID has
        been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(host_id, database_connection):
            return None

    host = info.retrieve_by_id(host_id,
                               database_connection,
                               pre_validated_id=True)
    host["appearances"] = core.retrieve_appearances_by_id(host_id,
                                                          database_connection,
                                                          pre_validated_id=True)
    return host

def retrieve_by_slug(host_slug: str,
                     database_connection: mysql.connector.connect
                    ) -> Dict:
    """Returns an OrderedDict with host details based on the requested
    host slug

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    host_id = utility.convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_by_id(host_id,
                              database_connection,
                              pre_validated_id=True)

    return None

def retrieve_all(database_connection: mysql.connector.connect
                        ) -> List[Dict]:
    """Returns a list of OrderedDicts with host details for all hosts

    Arguments:
        database_connection (mysql.connector.connect)
    """
    host_ids = info.retrieve_all_ids(database_connection)
    if not host_ids:
        return None

    hosts = []
    for host_id in host_ids:
        host = retrieve_by_id(host_id,
                              database_connection,
                              pre_validated_id=True)
        if host:
            hosts.append(host)

    return hosts

#endregion
