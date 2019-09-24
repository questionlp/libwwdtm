# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving guest information from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.guest import utility

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts containing guest information for
    all guests

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT guestid, guest, guestslug FROM ww_guests "
                 "WHERE guestslug != 'none' "
                 "ORDER BY guest ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        guests = []
        for row in result:
            guest = OrderedDict(id=row["guestid"],
                                name=row["guest"],
                                slug=row["guestslug"])
            guests.append(guest)

        return guests
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> List[int]:
    """Returns a list of all guest IDs, sorted by guest names

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT guestid FROM ww_guests WHERE guestslug != 'none' "
                 "ORDER BY guest ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        guest = []
        for row in result:
            guest.append(row[0])

        return guest
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_id(guest_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with guest information based on the
    requested guest ID

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(guest_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT guest, guestslug FROM ww_guests WHERE guestid = %s;")
        cursor.execute(query, (guest_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return OrderedDict(id=guest_id,
                               name=result["guest"],
                               slug=result["guestslug"])

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_slug(guest_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with guest information based on the
    requested guest slug

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
    """
    guest_id = utility.convert_slug_to_id(guest_slug, database_connection)
    if guest_id:
        return retrieve_by_id(guest_id,
                              database_connection,
                              pre_validated_id=True)

    return None

#endregion
