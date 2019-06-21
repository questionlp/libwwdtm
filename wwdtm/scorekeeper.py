# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_scorekeepers table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_appearances_by_id(scorekeeper_id: int,
                                database_connection: mysql.connector.connect,
                                pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested scorekeeper ID

    Arguments:
        scorekeeper_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the scorekeeper ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(scorekeeper_id, database_connection):
            return None

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

        appearance_counts = collections.OrderedDict()
        appearance_counts["regularShows"] = result["regular"]
        appearance_counts["allShows"] = result["allshows"]

        query = ("SELECT skm.showid, s.showdate, s.bestof, "
                 "s.repeatshowid, skm.guest, skm.description "
                 "FROM ww_showskmap skm "
                 "JOIN ww_scorekeepers sk ON "
                 "sk.scorekeeperid = skm.scorekeeperid "
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
                appearance_info["guest"] = bool(appearance["guest"])
                if appearance["description"]:
                    appearance_info["description"] = appearance["description"]
                else:
                    appearance_info["description"] = None

                appearances.append(appearance_info)

            appearance_dict["count"] = appearance_counts
            appearance_dict["shows"] = appearances
        else:
            appearance_dict["count"] = 0
            appearance_dict["shows"] = None

        return appearance_dict
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_appearances_by_slug(scorekeeper_slug: str,
                                  database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested scorekeeper slug

    Arguments:
        scorekeeper_slug (str)
        database_connection (mysql.connector.connect)
    """
    scorekeeper_id = convert_slug_to_id(scorekeeper_slug, database_connection)
    if scorekeeper_id:
        return _retrieve_appearances_by_id(scorekeeper_id, database_connection, True)

    return None

#endregion

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

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with scorekeeper information for
    all scorekeepers

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT scorekeeperid, scorekeeper, scorekeeperslug, "
                 "scorekeepergender "
                 "FROM ww_scorekeepers where scorekeeperslug != 'tbd' "
                 "ORDER BY scorekeeper ASC;")
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

        return scorekeepers
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> List[int]:
    """Returns a list of all scorekeeper IDs, sorted by scorekeeper
    names

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT scorekeeperid FROM ww_scorekeepers "
                 "WHERE scorekeeperslug != 'tbd' "
                 "ORDER BY scorekeeper ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        panelists = []
        for row in result:
            panelists.append(row[0])

        return panelists
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_id(scorekeeper_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with scorekeeper information for the
    requested scorekeeper ID

    Arguments:
        scorekeeper_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the scorekeeper ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(scorekeeper_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT scorekeeper, scorekeeperslug, "
                 "scorekeepergender "
                 "FROM ww_scorekeepers "
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
            return scorekeeper_dict

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_slug(scorekeeper_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with scorekeeper information for the
    requested scorekeeper slug

    Arguments:
        scorekeeper_slug (str)
        database_connection (mysql.connector.connect)
    """
    scorekeeper_id = convert_slug_to_id(scorekeeper_slug, database_connection)
    if scorekeeper_id:
        return retrieve_by_id(scorekeeper_id, database_connection, True)

    return None

def retrieve_details_by_id(scorekeeper_id: int,
                           database_connection: mysql.connector.connect,
                           pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with scorekeeper details for the
    requested scorekeeper ID

    Arguments:
        scorekeeper_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the scorekeeper ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(scorekeeper_id, database_connection):
            return None

    scorekeeper = retrieve_by_id(scorekeeper_id,
                                 database_connection,
                                 pre_validated_id=True)
    scorekeeper["appearances"] = _retrieve_appearances_by_id(scorekeeper_id,
                                                             database_connection,
                                                             pre_validated_id=True)
    return scorekeeper

def retrieve_details_by_slug(scorekeeper_slug: str,
                             database_connection: mysql.connector.connect
                            ) -> Dict:
    """Returns an OrderedDict with scorekeeper details based on the
    scorekeeper slug

    Arguments:
        scorekeeper_slug (str)
        database_connection (mysql.connector.connect): Database connect
        object
    """
    scorekeeper_id = convert_slug_to_id(scorekeeper_slug, database_connection)
    if scorekeeper_id:
        return retrieve_details_by_id(scorekeeper_id,
                                      database_connection,
                                      pre_validated_id=True)
    return None

def retrieve_all_details(database_connection: mysql.connector.connect
                        ) -> List[Dict]:
    """Returns a list of OrderedDicts with scorekeeper details for all
    scorekeepers

    Arguments:
        database_connection (mysql.connector.connect)
    """
    scorekeeper_ids = retrieve_all_ids(database_connection)
    if not scorekeeper_ids:
        return None

    scorekeepers = []
    for scorekeeper_id in scorekeeper_ids:
        scorekeeper = retrieve_details_by_id(scorekeeper_id,
                                             database_connection,
                                             pre_validated_id=True)
        if scorekeeper:
            scorekeepers.append(scorekeeper)

    return scorekeepers

#endregion
