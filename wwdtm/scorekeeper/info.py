# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving scorekeeper
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from slugify import slugify
from wwdtm.scorekeeper import utility

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
            info = OrderedDict()
            info["id"] = row["scorekeeperid"]
            info["name"] = row["scorekeeper"]
            if row["scorekeeperslug"]:
                info["slug"] = row["scorekeeperslug"]
            else:
                info["slug"] = slugify(info["name"])

            info["gender"] = row["scorekeepergender"]
            scorekeepers.append(info)

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
        if not utility.validate_id(scorekeeper_id, database_connection):
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
            scorekeeper_dict = OrderedDict()
            scorekeeper_dict["id"] = scorekeeper_id
            scorekeeper_dict["name"] = result["scorekeeper"]
            if result["scorekeeperslug"]:
                scorekeeper_dict["slug"] = result["scorekeeperslug"]
            else:
                scorekeeper_dict["slug"] = slugify(scorekeeper_dict["name"])

            scorekeeper_dict["gender"] = result["scorekeepergender"]
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
    scorekeeper_id = utility.convert_slug_to_id(scorekeeper_slug,
                                                database_connection)
    if scorekeeper_id:
        return retrieve_by_id(scorekeeper_id, database_connection, True)

    return None

#endregion
