# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving panelist
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Utility Functions
def convert_slug_to_id(panelist_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a panelist's slug based on the requested panelist ID

    Arguments:
        panelist_slug (str)
        database_connect (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistid FROM ww_panelists "
                 "WHERE panelistslug = %s;")
        cursor.execute(query, (panelist_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_id(panelist_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist ID is
    valid

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        panelist_id = int(panelist_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistid FROM ww_panelists "
                 "WHERE panelistid = %s;")
        cursor.execute(query, (panelist_id,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_slug(panelist_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist slug is
    valid

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_slug = panelist_slug.strip()
    if not panelist_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistslug FROM ww_panelists "
                 "WHERE panelistslug = %s;")
        cursor.execute(query, (panelist_slug,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(panelist_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist ID
    exists

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(panelist_id, database_connection)

def slug_exists(panelist_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist slug
    exists

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(panelist_slug, database_connection)

#endregion
