# -*- coding: utf-8 -*-
# Copyright (c) 2018-2020 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving location
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from slugify import slugify

#region Utility Functions
def convert_slug_to_id(location_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a host ID based on the requested location slug

    Arguments:
        location_slug (str)
        database_connect (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT locationid FROM ww_locations WHERE locationslug = %s;"
        cursor.execute(query, (location_slug,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

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

def validate_slug(location_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not the requested
    location slug is valid

    Arguments:
        location_slug (str)
        database_connection (mysql.connector.connect)
    """
    location_slug = location_slug.strip()
    if not location_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT locationslug FROM ww_locations WHERE locationslug = %s;"
        cursor.execute(query, (location_slug,))
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

def slug_exists(location_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a location slug
    exists

    Arguments:
        location_slug (str)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(location_slug, database_connection)

def slugify_location(location_id: int=None,
                     venue: str=None,
                     city: str=None,
                     state: str=None) -> str:
    """Generates a slug string based on the location's venue name,
    city, state and/or location ID.

    Arguments:
        location_id (int)
        venue (str)
        city (str)
        state (str)
    """
    if venue and city and state:
        return slugify("{} {} {}".format(venue, city, state))
    elif venue and city and not state:
        return slugify("{} {}".format(venue, city))
    elif id and venue and (not city and not state):
        return slugify("{} {}".format(id, venue))
    elif id and city and state and not venue:
        return slugify("{} {} {}".format(id, city, state))
    elif id:
        return "location-{}".format(location_id)
    else:
        raise ValueError("Invalid location information provided")

#endregion
