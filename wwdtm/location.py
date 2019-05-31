# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_locations table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_recordings_by_id(location_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing information about all of the
    show recordings made the requested location ID

    Arguments:
        location_id (int): Location ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the location ID has been validated
    Returns:
        list[OrderedDict]: Returns a list of OrderedDicts with location show recording
        information
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
            recording_info = collections.OrderedDict()
            recording_info["date"] = recording["showdate"].isoformat()
            recording_info["isBestOfShow"] = bool(recording["bestof"])
            recording_info["isShowRepeat"] = bool(recording["repeatshowid"])
            recordings.append(recording_info)

        return recordings
    except ProgrammingError as err:
        print("Unable to query the database: {}".format(err))
    except DatabaseError as err:
        print("Unexpected error: {}".format(err))

#endregion

#region Utility Functions
def validate_id(location_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Validate location ID against database

    Arguments:
        location_id (int): Location ID from database
        database_connection (mysql.connector.connect): Datbase connect object
    Returns:
        bool: Returns True on valid location ID, otherwise returns False
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT locationid FROM ww_locations WHERE locationid = %s;"
        cursor.execute(query, (location_id,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        print("Unable to query the database: {}".format(err))
    except DatabaseError as err:
        print("Unexpected error: {}".format(err))

def id_exists(location_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Return whether or not a location ID exists in the database

    Arguments:
        location_id (int): Location ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        bool: Returns True if location ID exists, otherwise returns False
    """
    return validate_id(location_id, database_connection)

#endregion

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns an OrderedDict with location information based on location ID

    Arguments:
        location_id (int): Location ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the location ID has been validated
    Returns:
        list[OrderedDict]: Returns an list of OrderedDicts containing location city, state and venue
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
            location_info = collections.OrderedDict()
            location_info["id"] = location["locationid"]
            location_info["city"] = location["city"]
            location_info["state"] = location["state"]
            location_info["venue"] = location["venue"]
            locations.append(location_info)

        return locations
    except ProgrammingError as err:
        print("Unable to query the database: {}".format(err))
    except DatabaseError as err:
        print("Unexpected error: {}".format(err))

def retrieve_all_ids(database_connection: mysql.connector.connect) -> List[int]:
    """Return a list of all location ID sorted in the order of state, city and venue

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[int]: List containing location IDs
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
        print("Unable to query the database: {}".format(err))
    except DatabaseError as err:
        print("Unexpected error: {}".format(err))

def retrieve_by_id(location_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with location information based on location ID

    Arguments:
        location_id (int): Location ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the location ID has been validated
    Returns:
        OrderedDict: Returns an OrderedDict containing location city, state and venue
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

        location_info = collections.OrderedDict()
        location_info["id"] = result["locationid"]
        location_info["city"] = result["city"]
        location_info["state"] = result["state"]
        location_info["venue"] = result["venue"]

        return location_info
    except ProgrammingError as err:
        print("Unable to query the database: {}".format(err))
    except DatabaseError as err:
        print("Unexpected error: {}".format(err))
    return None

def retrieve_recordings_by_id(location_id: int,
                              database_connection: mysql.connector.connect,
                              pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with location information and recordings based on location ID

    Arguments:
        location_id (int): Location ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the location ID has been validated
    Returns:
        OrderedDict: Returns an OrderedDict containing location city, state, venue and recordings
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

def retrieve_all_recordings(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with location information and recordings based on location ID

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[OrderedDict]: Returns a list of OrderedDicts containing location city, state, venue and recordings
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