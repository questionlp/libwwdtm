# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_guests table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_appearances_by_id(guest_id: int,
                                database_connection: mysql.connector.connect,
                                pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested guest ID

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    if not pre_validated_id:
        valid_id = validate_id(guest_id, database_connection)
        if not valid_id:
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT ( "
                 "SELECT COUNT(gm.showid) FROM ww_showguestmap gm "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE s.bestof = 0 AND s.repeatshowid IS NULL AND "
                 "gm.guestid = %s ) AS regular, ( "
                 "SELECT COUNT(gm.showid) FROM ww_showguestmap gm "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE gm.guestid = %s ) AS allshows;")
        cursor.execute(query, (guest_id, guest_id,))
        result = cursor.fetchone()

        appearance_counts = OrderedDict(regular_shows=result["regular"],
                                        all_shows=result["allshows"])

        query = ("SELECT gm.showid, s.showdate, s.bestof, s.repeatshowid, "
                 "gm.guestscore, gm.exception FROM ww_showguestmap gm "
                 "JOIN ww_guests g ON g.guestid = gm.guestid "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE gm.guestid = %s "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (guest_id,))
        result = cursor.fetchall()
        cursor.close()

        if result:
            appearances = []
            for appearance in result:
                appearance_info = OrderedDict(date=appearance["showdate"].isoformat(),
                                              best_of=bool(appearance["bestof"]),
                                              repeat_show=bool(appearance["repeatshowid"]),
                                              score=appearance["guestscore"],
                                              score_exception=bool(appearance["exception"]))
                appearances.append(appearance_info)

            return OrderedDict(count=appearance_counts, shows=appearances)

        return OrderedDict(count=0, shows=None)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_appearances_by_slug(guest_slug: str,
                                  database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested guest slug

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
    """
    guest_id = convert_slug_to_id(guest_slug, database_connection)
    if guest_id:
        return _retrieve_appearances_by_id(guest_id, database_connection, True)

    return None

#endregion

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
        if not validate_id(guest_id, database_connection):
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
    guest_id = convert_slug_to_id(guest_slug, database_connection)
    if guest_id:
        return retrieve_by_id(guest_id,
                              database_connection,
                              pre_validated_id=True)

    return None

def retrieve_details_by_id(guest_id: int,
                           database_connection: mysql.connector.connect,
                           pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with guest details based on the
    requested guest ID

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    if not pre_validated_id:
        if not validate_id(guest_id, database_connection):
            return None

    guest = retrieve_by_id(guest_id,
                           database_connection,
                           pre_validated_id=True)
    guest["appearances"] = _retrieve_appearances_by_id(guest_id,
                                                       database_connection,
                                                       pre_validated_id=True)
    return guest

def retrieve_details_by_slug(guest_slug: str,
                             database_connection: mysql.connector.connect
                            ) -> Dict:
    """Returns an OrderedDict with guest details based on the
    requested guest slug

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    guest_id = convert_slug_to_id(guest_slug, database_connection)
    if guest_id:
        return retrieve_details_by_id(guest_id,
                                      database_connection,
                                      pre_validated_id=True)

    return None

def retrieve_all_details(database_connection: mysql.connector.connection
                        ) -> List[Dict]:
    """Returns a list of OrderedDict with guest details for all guests

    Arguments:
        database_connection (mysql.connector.connect)
    """
    guest_ids = retrieve_all_ids(database_connection)
    if not guest_ids:
        return None

    guests = []
    for guest_id in guest_ids:
        guest_details = retrieve_details_by_id(guest_id,
                                               database_connection,
                                               pre_validated_id=True)
        if guest_details:
            guests.append(guest_details)

    return guests

#endregion
