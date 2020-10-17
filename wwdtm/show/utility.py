# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving show
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import datetime
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Utility Functions
def validate_id(show_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on wheter or not a show ID is valid

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        show_id = int(show_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT showid from ww_shows where showid = %s;"
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def convert_date_to_id(show_year: int,
                       show_month: int,
                       show_day: int,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a show's ID based on the show's year, month and day

    Arguments:
        show_year (int): Four digit year is required
        show_month (int)
        show_day (int)
        database_connection (mysql.connector.connect)
    """
    show_date = None
    try:
        show_date = datetime.datetime(year=show_year,
                                      month=show_month,
                                      day=show_day)
    except ValueError as err:
        raise ValueError("Invalid year, month and/or day value") from err

    try:
        show_date_str = show_date.isoformat()
        cursor = database_connection.cursor()
        query = "SELECT showid from ww_shows WHERE showdate = %s;"
        cursor.execute(query, (show_date_str,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def convert_id_to_date(show_id: int,
                       database_connection: mysql.connector.connect
                      ) -> datetime.datetime:
    """Returns a show's date based on the show's ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT showdate FROM ww_shows WHERE showid = %s;"
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0].isoformat()

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(show_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a show ID exists

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(show_id, database_connection)

def date_exists(show_year: int,
                show_month: int,
                show_day: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a show exists for
    the requested year, month and day

    Arguments:
        show_year (int): Four digit year is required
        show_month (int)
        show_day (int)
        database_connection (mysql.connector.connect)
    """
    show_date = None
    try:
        show_date = datetime.datetime(show_year, show_month, show_day)
    except ValueError as err:
        raise ValueError("Invalid year, month and/or day value") from err

    try:
        show_date_str = show_date.isoformat()
        cursor = database_connection.cursor()
        query = "SELECT showid from ww_shows WHERE showdate = %s;"
        cursor.execute(query, (show_date_str,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
