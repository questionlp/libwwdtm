# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_guests table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict, Tuple
import mysql.connector
from wwdtm.responsecode import ResponseCode

def convert_slug_to_id(guest_slug: str,
                       database_connection: mysql.connector.connect
                      ) -> Tuple[int, ResponseCode]:
    """Return guest database ID from slug string.

    Arguments:
        guest_slug (str): Guest slug string
        database_connect (mysql.connector.connect): Database connect object

    Returns:
        (int, ResponseCode): Returns guest ID on success; otherwise, it will return
        None. Also returns a ReponseCode IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT guestid FROM ww_guests WHERE guestslug = %s;"
        cursor.execute(query, (guest_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return result["guestid"], ResponseCode.SUCCESS

        return None, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def validate_id(guest_id: int,
                database_connection: mysql.connector.connect) -> Tuple[bool, ResponseCode]:
    """Validate guest ID against database

    Arguments:
        guest_id (int); Guest ID from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        (bool, ResponseCode): Returns True on success; otherwise, it will return False if not
        found. Also returns a ReponseCode IntEnum
    """
    try:
        guest_id = int(guest_id)
    except ValueError:
        return False, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT guestid FROM ww_guests WHERE guestid = %s;"
        cursor.execute(query, (guest_id,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, ResponseCode.SUCCESS

        return False, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def validate_slug(guest_slug: str,
                  database_connection: mysql.connector.connect) -> Tuple[bool, ResponseCode]:
    """Validate guest slug string against database

    Arguments:
        guest_slug (str): Guest slug string from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        (bool, ResponseCode): Returns True if guest slug is valid, False otherwise.
        Also returns a ReponseCode IntEnum
    """
    guest_slug = guest_slug.strip()
    if not guest_slug:
        return False, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT guestslug FROM ww_guests WHERE guestslug = %s;"
        cursor.execute(query, (guest_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, ResponseCode.SUCCESS

        return False, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def id_exists(guest_id: int,
              database_connection: mysql.connector.connect) -> Tuple[bool, ResponseCode]:
    """Return whether or not a guest ID exists in the database.

    Arguments:
        guest_id (int): Guest ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns True if guest ID exists, False otherwise. Also
        returns a ReponseCode IntEnum
    """
    return validate_id(guest_id, database_connection)

def slug_exists(guest_slug: str,
                database_connection: mysql.connector.connect) -> Tuple[bool, ResponseCode]:
    """Return whether or not a guest slug exists in the database.

    Arguments:
        guest_slug (int): Guest slug from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns True if guest slug exists, False otherwise.
        Also returns a ReponseCode IntEnum
    """
    return validate_slug(guest_slug, database_connection)

def retrieve_all(database_connection: mysql.connector.connect) -> Tuple[List[Dict], ResponseCode]:
    """Return a list of OrderedDicts containing guests and their details.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict of guest
        details. Also returns a ReponseCode IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT guestid, guest, guestslug FROM ww_guests WHERE guestslug != 'none' "
                 "ORDER BY guest ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        guests = []
        for row in result:
            guest = collections.OrderedDict()
            guest["id"] = row["guestid"]
            guest["name"] = row["guest"]
            guest["slug"] = row["guestslug"]
            guests.append(guest)

        return guests, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> Tuple[List[int], ResponseCode]:
    """Return a list of all guest IDs, with IDs sorted in the order of guest names.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[int], ResponseCode): Returns a list containing guest IDs. Also returns a ReponseCode
        IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT guestid FROM ww_guests WHERE guestslug != 'none' ORDER BY guest ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        panelists = []
        for row in result:
            panelists.append(row["guestid"])

        return panelists, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_by_id(guest_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Tuple[Dict, ResponseCode]:
    """Returns an OrderedDict with guest details based on the guest ID.

    Arguments:
        guest_id (int): Guest ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the guest ID has been validated or not
    Returns:
        (OrderedDict, ResponseCode): Returns a dict containing guest id, name, and slug string.
        Also returns a ReponseCode IntEnum
    """
    (valid_id, response_code) = validate_id(guest_id, database_connection)
    if (not pre_validated_id and not valid_id):
        return None, response_code

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT guest, guestslug FROM ww_guests WHERE guestid = %s;")

        cursor.execute(query, (guest_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            guest_dict = collections.OrderedDict()
            guest_dict = {
                "id": guest_id,
                "name": result["guest"],
                "slug": result["guestslug"]
                }
            return guest_dict, ResponseCode.SUCCESS

        return None, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_by_slug(guest_slug: str,
                     database_connection: mysql.connector.connect) -> Tuple[Dict, ResponseCode]:
    """Returns an OrderedDict with guest details based on the guest slug string

    Arguments:
        guest_slug (str): Guest slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (OrderedDict, ResponseCode): Returns an OrderedDict containing guest id, name and slug
        string. Also returns a ReponseCode IntEnum
    """
    (guest_id, response_code) = convert_slug_to_id(guest_slug, database_connection)
    if not guest_id:
        return None, response_code

    return retrieve_by_id(guest_id, database_connection, True)

def retrieve_appearances_by_id(guest_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> Tuple[List[Dict], ResponseCode]:
    """Returns a list of OrderedDicts containing information about all of the guest's
    appearances.

    Arguments:
        guest_id (int): Guest ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the guest ID has been validated or not
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict with guest
        appearance information. Also returns a ReponseCode IntEnum
    """
    if not pre_validated_id:
        (valid_id, response_code) = validate_id(guest_id, database_connection)
        if not valid_id:
            return None, response_code

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
        cursor.close()

        appearance_counts = collections.OrderedDict()
        appearance_counts["regularShows"] = result["regular"]
        appearance_counts["allShows"] = result["allshows"]

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT gm.showid, s.showdate, s.bestof, s.repeatshowid, "
                 "gm.guestscore, gm.exception FROM ww_showguestmap gm "
                 "JOIN ww_guests g ON g.guestid = gm.guestid "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE gm.guestid = %s "
                 "ORDER BY s.showdate ASC;")

        cursor.execute(query, (guest_id,))
        result = cursor.fetchall()
        cursor.close()

        appearance_dict = collections.OrderedDict()
        if result:
            appearances = []
            for appearance in result:
                appearance_info = {}
                appearance_info["date"] = appearance["showdate"].isoformat()
                appearance_info["isBestOfShow"] = bool(appearance["bestof"])
                appearance_info["isShowRepeat"] = bool(appearance["repeatshowid"])
                appearance_info["score"] = appearance["guestscore"]
                appearance_info["exception"] = bool(appearance["exception"])
                appearances.append(appearance_info)

            appearance_dict["count"] = appearance_counts
            appearance_dict["shows"] = appearances
        else:
            appearance_dict["count"] = 0
            appearance_dict["shows"] = None

        return appearance_dict, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_appearances_by_slug(guest_slug: str,
                                 database_connection: mysql.connector.connect
                                ) -> Tuple[List[Dict], ResponseCode]:
    """Returns a list of OrderedDicts containing information about all of the guest's
    appearances.

    Arguments:
        guest_slug (str): Guest slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict with guest
        appearance information. Also returns a ReponseCode IntEnum
    """
    (guest_id, response_code) = convert_slug_to_id(guest_slug, database_connection)
    if not guest_id:
        return None, response_code

    return retrieve_appearances_by_id(guest_id, database_connection, True)
