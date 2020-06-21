# -*- coding: utf-8 -*-
# Copyright (c) 2018-2020 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving location information
from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.location import utility

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect,
                 sort_by_venue: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts with location information for all
    locations, ordered by state, then city, then venue

    Arguments:
        location_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        # Exclude any entries that are considered to be fully TBD
        query = ("SELECT locationid, locationslug, city, state, venue "
                 "FROM ww_locations "
                 "WHERE locationid NOT IN (3) ")

        if sort_by_venue:
            query = query + "ORDER BY venue ASC, state ASC, city ASC;"
        else:
            query = query + "ORDER BY state ASC, city ASC, venue ASC;"

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        locations = []
        for location in result:
            location_info = OrderedDict()
            location_info["id"] = location["locationid"]
            location_info["slug"] = location["locationslug"]
            location_info["city"] = location["city"]
            location_info["state"] = location["state"]
            location_info["venue"] = location["venue"]
            locations.append(location_info)

        return locations
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect,
                     sort_by_venue: bool = False
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
                 "WHERE locationid NOT IN (3) ")

        if sort_by_venue:
            query = query + "ORDER BY venue ASC, state ASC, city ASC;"
        else:
            query = query + "ORDER BY state ASC, city ASC, venue ASC;"

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
        if not utility.validate_id(location_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        # Exclude any entries that are considered to be fully TBD
        query = ("SELECT locationid, city, state, venue, locationslug "
                 "FROM ww_locations "
                 "WHERE locationid = %s; ")
        cursor.execute(query, (location_id,))
        result = cursor.fetchone()
        cursor.close()

        location_info = OrderedDict()
        location_info["id"] = result["locationid"]
        location_info["slug"] = result["locationslug"]
        location_info["city"] = result["city"]
        location_info["state"] = result["state"]
        location_info["venue"] = result["venue"]
        return location_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_slug(location_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with host information based on the
    requested location slug

    Arguments:
        location_slug (str)
        database_connection (mysql.connector.connect)
    """
    location_id = utility.convert_slug_to_id(location_slug, database_connection)
    if location_id:
        return retrieve_by_id(location_id, database_connection, True)

    return None

#endregion
