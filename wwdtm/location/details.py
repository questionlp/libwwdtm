# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving location details from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from typing import List, Dict
import mysql.connector
from wwdtm.location import core, info, utility

#region Retrieval Functions
def retrieve_recordings_by_id(location_id: int,
                              database_connection: mysql.connector.connect,
                              pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with location recordings based on the
    requested location ID

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the location ID
        has been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(location_id, database_connection):
            return None

    location = info.retrieve_by_id(location_id,
                                   database_connection,
                                   pre_validated_id=True)
    recordings = core.retrieve_recordings_by_id(location_id,
                                                database_connection,
                                                pre_validated_id=True)
    location["recordings"] = recordings
    return location

def retrieve_all_recordings(database_connection: mysql.connector.connect
                           ) -> List[Dict]:
    """Returns a list of OrderedDicts with location information and
    recordings based on location ID

    Arguments:
        database_connection (mysql.connector.connect): Database connect
        object
    Returns:
        list[OrderedDict]: Returns a list of OrderedDicts containing
        location city, state, venue and recordings
    """
    location_ids = info.retrieve_all_ids(database_connection)
    if not location_ids:
        return None

    locations = []
    for location_id in location_ids:
        location = retrieve_recordings_by_id(location_id,
                                             database_connection,
                                             pre_validated_id=True)
        if location:
            locations.append(location)

    return locations

#endregion
