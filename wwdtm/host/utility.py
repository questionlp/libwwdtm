# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving host details
from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Utility Functions
def convert_slug_to_id(host_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a host ID based on the requested host slug

    Arguments:
        host_slug (str)
        database_connect (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT hostid FROM ww_hosts WHERE hostslug = %s;"
        cursor.execute(query, (host_slug,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_id(host_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not the requested host
    ID is valid

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        host_id = int(host_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT hostid FROM ww_hosts WHERE hostid = %s;"
        cursor.execute(query, (host_id,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_slug(host_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not the requested host
    slug is valid

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    host_slug = host_slug.strip()
    if not host_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT hostslug FROM ww_hosts WHERE hostslug = %s;"
        cursor.execute(query, (host_slug,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(host_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on whether or not a host ID exists

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(host_id, database_connection)

def slug_exists(host_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on whether or not a host slug
    exists

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(host_slug, database_connection)

#endregion
