# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving guest
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Utility Functions
def convert_slug_to_id(guest_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a guest ID based on the requested guest slug

    Arguments:
        guest_slug (str)
        database_connect (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT guestid FROM ww_guests WHERE guestslug = %s;"
        cursor.execute(query, (guest_slug,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_id(guest_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on wheter or not a guest ID is valid

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        guest_id = int(guest_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT guestid FROM ww_guests WHERE guestid = %s;"
        cursor.execute(query, (guest_id,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_slug(guest_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on wheter or not a guest slug is
    valid

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
    """
    guest_slug = guest_slug.strip()
    if not guest_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT guestslug FROM ww_guests WHERE guestslug = %s;"
        cursor.execute(query, (guest_slug,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(guest_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on wheter or not a guest ID exists

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(guest_id, database_connection)

def slug_exists(guest_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on wheter or not a guest slug exists

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(guest_slug, database_connection)

#endregion
