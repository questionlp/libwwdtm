# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides utility functions for retrieving location
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

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
