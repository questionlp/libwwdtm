# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving panelist details from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from typing import List, Dict
import mysql.connector
from wwdtm.panelist import core, info, utility

#region Retrieval Functions
def retrieve_by_id(panelist_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with panelist details based on the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(panelist_id, database_connection):
            return None

    panelist = info.retrieve_by_id(panelist_id,
                                   database_connection,
                                   pre_validated_id=True)
    statistics = core.retrieve_statistics_by_id(panelist_id,
                                                database_connection,
                                                pre_validated_id=True)
    panelist["statistics"] = statistics
    bluff_statistics = core.retrieve_bluffs_by_id(panelist_id,
                                                  database_connection,
                                                  pre_validated_id=True)
    panelist["bluffs"] = bluff_statistics
    appearances = core.retrieve_appearances_by_id(panelist_id,
                                                  database_connection,
                                                  pre_validated_id=True)
    panelist["appearances"] = appearances
    return panelist

def retrieve_by_slug(panelist_slug: str,
                     database_connection: mysql.connector.connect
                    ) -> Dict:
    """Returns an OrderedDict with panelist details based on the
    requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = utility.convert_slug_to_id(panelist_slug, database_connection)
    if not panelist_id:
        return None

    return retrieve_by_id(panelist_id,
                          database_connection,
                          pre_validated_id=True)

def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with panelist details for all
    panelists

    Arguments:
        database_connection (mysql.connector.connect)
    """
    panelist_ids = info.retrieve_all_ids(database_connection)
    if not panelist_ids:
        return None

    panelists = []
    for panelist_id in panelist_ids:
        panelist = retrieve_by_id(panelist_id,
                                  database_connection,
                                  pre_validated_id=True)
        if panelist:
            panelists.append(panelist)

    return panelists

#endregion
