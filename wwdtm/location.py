# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_locations table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_recordings_by_id(location_id: int,
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
        if not validate_id(location_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, s.bestof, s.repeatshowid "
                 "FROM ww_showlocationmap lm "
                 "JOIN ww_shows s ON s.showid = lm.showid "
                 "WHERE lm.locationid = %s "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (location_id,))
        result = cursor.fetchall()
        cursor.close()

        recordings = []
        for recording in result:
            recording_info = OrderedDict(date=recording["showdate"].isoformat(),
                                         isBestOfShow=bool(recording["bestof"]),
                                         isShowRepeat=bool(recording["repeatshowid"]))
            recordings.append(recording_info)

        return recordings
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion

#region Utility Functions
def validate_id(location_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a location ID is
    valid

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT locationid FROM ww_locations WHERE locationid = %s;"
        cursor.execute(query, (location_id,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(location_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a location ID
    exists

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(location_id, database_connection)

#endregion

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with location information for all
    locations, ordered by state, then city, then venue

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        # Exclude any entries that are considered to be fully TBD
        query = ("SELECT locationid, city, state, venue "
                 "FROM ww_locations "
                 "WHERE locationid NOT IN (3) "
                 "ORDER BY state ASC, city ASC, venue ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        locations = []
        for location in result:
            location_info = OrderedDict(id=location["locationid"],
                                        city=location["city"],
                                        state=location["state"],
                                        venue=location["venue"])
            locations.append(location_info)

        return locations
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> List[int]:
    """Returns a list of location IDs for all locations, ordered by
    state, then city, then venue

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        # Exclude any entries that are considered to be fully TBD
        query = ("SELECT locationid FROM ww_locations "
                 "WHERE locationid NOT IN (3) "
                 "ORDER BY state ASC, city ASC, venue ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        locations = []
        for row in result:
            locations.append(row[0])

        return locations
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_id(location_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with location information based on
    requested location ID

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the location ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(location_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        # Exclude any entries that are considered to be fully TBD
        query = ("SELECT locationid, city, state, venue "
                 "FROM ww_locations "
                 "WHERE locationid = %s; ")
        cursor.execute(query, (location_id,))
        result = cursor.fetchone()
        cursor.close()

        return OrderedDict(id=result["locationid"],
                           city=result["city"],
                           state=result["state"],
                           venue=result["venue"])
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

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
        if not validate_id(location_id, database_connection):
            return None

    location = retrieve_by_id(location_id,
                              database_connection,
                              pre_validated_id=True)
    location["recordings"] = _retrieve_recordings_by_id(location_id,
                                                        database_connection,
                                                        pre_validated_id=True)
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
    location_ids = retrieve_all_ids(database_connection)
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
