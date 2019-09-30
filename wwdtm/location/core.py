# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides core functions for retrieving location
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.location import utility

#region Internal Functions
def retrieve_recordings_by_id(location_id: int,
                              database_connection: mysql.connector.connect,
                              pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing recording information
    for the requested location ID

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the location ID
        has been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(location_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT lm.showid, s.showdate, s.bestof, s.repeatshowid "
                 "FROM ww_showlocationmap lm "
                 "JOIN ww_shows s ON s.showid = lm.showid "
                 "WHERE lm.locationid = %s "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (location_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        recordings = []
        for recording in result:
            recording_info = OrderedDict(show_id=recording["showid"],
                                         date=recording["showdate"].isoformat(),
                                         best_of=bool(recording["bestof"]),
                                         repeat_show=bool(recording["repeatshowid"]))
            recordings.append(recording_info)

        return recordings
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
