# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_scorekeepers table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict, Tuple
import mysql.connector
from wwdtm.responsecode import ResponseCode

def convert_slug_to_id(scorekeeper_slug: str,
                       database_connection: mysql.connector.connect
                      ) -> Tuple[int, ResponseCode]:
    """Return scorekeeper database ID from slug string.

    Arguments:
        scorekeeper_slug (str): Scorekeeper slug string
        database_connect (mysql.connector.connect): Database connect object

    Returns:
        (int, ResponseCode): Returns scorekeeper ID on success; otherwise, it will return
        None. Also returns a ReponseCode IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT scorekeeperid FROM ww_scorekeepers WHERE scorekeeperslug = %s;"
        cursor.execute(query, (scorekeeper_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return result["scorekeeperid"], ResponseCode.SUCCESS

        return None, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def validate_id(scorekeeper_id: int,
                database_connection: mysql.connector.connect
               ) -> Tuple[bool, ResponseCode]:
    """Validate scorekeeper ID against database

    Arguments:
        scorekeeper_id (int); Scorekeeper ID from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        (bool, ResponseCode): Returns True on success; otherwise, it will return False if not
        found. Also returns a ReponseCode IntEnum
    """
    try:
        scorekeeper_id = int(scorekeeper_id)
    except ValueError:
        return False, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT scorekeeperid FROM ww_scorekeepers WHERE scorekeeperid = %s;"
        cursor.execute(query, (scorekeeper_id,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, ResponseCode.SUCCESS

        return False, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def validate_slug(scorekeeper_slug: str,
                  database_connection: mysql.connector.connect
                 ) -> Tuple[bool, ResponseCode]:
    """Validate scorekeeper slug string against database

    Arguments:
        scorekeeper_slug (str): Scorekeeper slug string from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        (bool, ResponseCode): Returns True if scorekeeper slug is valid, False otherwise.
        Also returns a ReponseCode IntEnum
    """
    scorekeeper_slug = scorekeeper_slug.strip()
    if not scorekeeper_slug:
        return False, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT scorekeeperslug FROM ww_scorekeepers WHERE scorekeeperslug = %s;"
        cursor.execute(query, (scorekeeper_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, ResponseCode.SUCCESS

        return False, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def id_exists(scorekeeper_id: int,
              database_connection: mysql.connector.connect
             ) -> Tuple[bool, ResponseCode]:
    """Return whether or not a scorekeeper ID exists in the database.

    Arguments:
        scorekeeper_id (int): Host ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns True if scorekeeper ID exists, False otherwise. Also
        returns a ReponseCode IntEnum
    """
    return validate_id(scorekeeper_id, database_connection)

def slug_exists(scorekeeper_slug: str,
                database_connection: mysql.connector.connect
               ) -> Tuple[bool, ResponseCode]:
    """Return whether or not a scorekeeper slug exists in the database.

    Arguments:
        scorekeeper_slug (int): Host slug from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns True if scorekeeper slug exists, False otherwise.
                              Also returns a ReponseCode IntEnum
    """
    return validate_slug(scorekeeper_slug, database_connection)

def retrieve_all(database_connection: mysql.connector.connect
                ) -> Tuple[List[Dict], ResponseCode]:
    """Return a list of OrderedDicts containing scorekeepers and their details.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict of scorekeeper
        details. Also returns a ReponseCode IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT scorekeeperid, scorekeeper, scorekeeperslug, scorekeepergender "
                 "FROM ww_scorekeepers where scorekeeperslug != 'tbd' ORDER BY scorekeeper ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        scorekeepers = []
        for row in result:
            scorekeeper = collections.OrderedDict()
            scorekeeper["id"] = row["scorekeeperid"]
            scorekeeper["name"] = row["scorekeeper"]
            scorekeeper["slug"] = row["scorekeeperslug"]
            scorekeeper["gender"] = row["scorekeepergender"]
            scorekeepers.append(scorekeeper)

        return scorekeepers, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> Tuple[List[int], ResponseCode]:
    """Return a list of all scorekeeper IDs, with IDs sorted in the order of scorekeeper names.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[int], ResponseCode): Returns a list containing scorekeeper IDs. Also returns a
        ReponseCode IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT scorekeeperid FROM ww_scorekeepers WHERE scorekeeperslug != 'tbd' "
                 "ORDER BY scorekeeper ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        panelists = []
        for row in result:
            panelists.append(row["scorekeeperid"])

        return panelists, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_by_id(scorekeeper_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False
                  ) -> Tuple[Dict, ResponseCode]:
    """Returns an OrderedDict with scorekeeper details based on the scorekeeper ID.

    Arguments:
        scorekeeper_id (int): Scorekeeper ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the scorekeeper ID has been validated or not
    Returns:
        (OrderedDict, ResponseCode): Returns a dict containing scorekeeper id, name, and slug
        string. Also returns a ReponseCode IntEnum
    """
    if not pre_validated_id:
        (valid_id, response_code) = validate_id(scorekeeper_id, database_connection)
        if not valid_id:
            return None, response_code

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT scorekeeper, scorekeeperslug, scorekeepergender FROM ww_scorekeepers "
                 "WHERE scorekeeperid = %s;")

        cursor.execute(query, (scorekeeper_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            scorekeeper_dict = collections.OrderedDict()
            scorekeeper_dict = {
                "id": scorekeeper_id,
                "name": result["scorekeeper"],
                "slug": result["scorekeeperslug"],
                "gender": result["scorekeepergender"]
                }
            return scorekeeper_dict, ResponseCode.SUCCESS

        return None, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_by_slug(scorekeeper_slug: str,
                     database_connection: mysql.connector.connect
                    ) -> Tuple[Dict, ResponseCode]:
    """Returns an OrderedDict with scorekeeper details based on the scorekeeper slug string

    Arguments:
        scorekeeper_slug (str): Scorekeeper slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (OrderedDict, ResponseCode): Returns a dict containing scorekeeper id, name and slug
        string. Also returns a ReponseCode IntEnum
    """
    (scorekeeper_id, response_code) = convert_slug_to_id(scorekeeper_slug, database_connection)
    if not scorekeeper_id:
        return None, response_code

    return retrieve_by_id(scorekeeper_id, database_connection, True)

def retrieve_appearances_by_id(scorekeeper_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False
                              ) -> Tuple[List[Dict], ResponseCode]:
    """Returns a list of OrderedDicts containing information about all of the scorekeeper's
    appearances.

    Arguments:
        scorekeeper_id (int): Scorekeeper ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the scorekeeper ID has been validated or not
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict with
        scorekeeper appearance information. Also returns a ReponseCode IntEnum
    """
    if not pre_validated_id:
        (valid_id, response_code) = validate_id(scorekeeper_id, database_connection)
        if not valid_id:
            return None, response_code

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT ( "
                 "SELECT COUNT(skm.showid) FROM ww_showskmap skm "
                 "JOIN ww_shows s ON s.showid = skm.showid "
                 "WHERE s.bestof = 0 AND s.repeatshowid IS NULL AND "
                 "skm.scorekeeperid = %s ) AS regular, ( "
                 "SELECT COUNT(skm.showid) FROM ww_showskmap skm "
                 "JOIN ww_shows s ON s.showid = skm.showid "
                 "WHERE skm.scorekeeperid = %s ) AS allshows;")
        cursor.execute(query, (scorekeeper_id, scorekeeper_id,))
        result = cursor.fetchone()
        cursor.close()

        appearance_counts = collections.OrderedDict()
        appearance_counts["regularShows"] = result["regular"]
        appearance_counts["allShows"] = result["allshows"]

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT skm.showid, s.showdate, s.bestof, s.repeatshowid, skm.guest, "
                 "skm.description FROM ww_showskmap skm "
                 "JOIN ww_scorekeepers sk ON sk.scorekeeperid = skm.scorekeeperid "
                 "JOIN ww_shows s ON s.showid = skm.showid "
                 "WHERE skm.scorekeeperid = %s "
                 "ORDER BY s.showdate ASC;")

        cursor.execute(query, (scorekeeper_id,))
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
                appearance_info["exception"] = bool(appearance["guest"])
                appearance_info["description"] = appearance["description"]
                appearances.append(appearance_info)

            appearance_dict["count"] = appearance_counts
            appearance_dict["shows"] = appearances
        else:
            appearance_dict["count"] = 0
            appearance_dict["shows"] = None

        return appearance_dict, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_appearances_by_slug(scorekeeper_slug: str,
                                 database_connection: mysql.connector.connect
                                ) -> Tuple[List[Dict], ResponseCode]:
    """Returns a list of OrderedDicts containing information about all of the scorekeeper's
    appearances.

    Arguments:
        scorekeeper_slug (str): Scorekeeper slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict with
        scorekeeper appearance information. Also returns a ReponseCode IntEnum
    """
    (scorekeeper_id, response_code) = convert_slug_to_id(scorekeeper_slug, database_connection)
    if not scorekeeper_id:
        return None, response_code

    return retrieve_appearances_by_id(scorekeeper_id, database_connection, True)
