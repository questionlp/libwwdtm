# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving scorekeeper
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Utility Functions
def convert_slug_to_id(scorekeeper_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a scorekeeper ID based on the scorekeeper's slug

    Arguments:
        scorekeeper_slug (str)
        database_connect (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT scorekeeperid FROM ww_scorekeepers "
                 "WHERE scorekeeperslug = %s;")
        cursor.execute(query, (scorekeeper_slug,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_id(scorekeeper_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on wheter or not a scorekeeper ID
    is valid

    Arguments:
        scorekeeper_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        scorekeeper_id = int(scorekeeper_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = ("SELECT scorekeeperid FROM ww_scorekeepers "
                 "WHERE scorekeeperid = %s;")
        cursor.execute(query, (scorekeeper_id,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_slug(scorekeeper_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on wheter or not a scorekeeper slug
    is valid

    Arguments:
        scorekeeper_slug (str)
        database_connection (mysql.connector.connect)
    """
    scorekeeper_slug = scorekeeper_slug.strip()
    if not scorekeeper_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = ("SELECT scorekeeperslug FROM ww_scorekeepers "
                 "WHERE scorekeeperslug = %s;")
        cursor.execute(query, (scorekeeper_slug,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(scorekeeper_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on wheter or not a scorekeeper ID
    exists

    Arguments:
        scorekeeper_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(scorekeeper_id, database_connection)

def slug_exists(scorekeeper_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on wheter or not a scorekeeper slug
    exists

    Arguments:
        scorekeeper_slug (int)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(scorekeeper_slug, database_connection)

#endregion
