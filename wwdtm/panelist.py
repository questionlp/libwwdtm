# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_panelists table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict
import mysql.connector
import numpy

def convert_slug_to_id(panelist_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Return panelist database ID from slug string.

    Arguments:
        panelist_slug (str): Panelist slug string
        database_connect (mysql.connector.connect): Database connect object

    Returns:
        int: Returns panelist ID on success, otherwise returns None
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT panelistid FROM ww_panelists WHERE panelistslug = %s;"
        cursor.execute(query, (panelist_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return result["panelistid"]

        return None
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def validate_id(panelist_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Validate panelist ID against database

    Arguments:
        panelist_id (int); Panelist ID from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        bool: Returns True on success, otherwise returns False
    """
    try:
        panelist_id = int(panelist_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT panelistid FROM ww_panelists WHERE panelistid = %s;"
        cursor.execute(query, (panelist_id,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def validate_slug(panelist_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Validate panelist slug string against database

    Arguments:
        panelist_slug (str): Panelist slug string from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        bool: Returns True if panelist slug is valid, otherwise returns False
    """
    panelist_slug = panelist_slug.strip()
    if not panelist_slug:
        return False

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT panelistslug FROM ww_panelists WHERE panelistslug = %s;"
        cursor.execute(query, (panelist_slug,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def id_exists(panelist_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Return whether or not a panelist ID exists in the database.

    Arguments:
        panelist_id (int): Panelist ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        bool: Returns True if panelist ID exists, otherwise returns False
    """
    return validate_id(panelist_id, database_connection)

def slug_exists(panelist_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Return whether or not a panelist slug exists in the database.

    Arguments:
        panelist_slug (int): Panelist slug from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        bool: Returns True if panelist slug exists, otherwise returns False
    """
    return validate_slug(panelist_slug, database_connection)

def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Return a list of OrderedDicts containing panelists and their details.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[OrderedDict]: Returns a list containing an OrderedDict containing
        panelist details
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT panelistid, panelist, panelistslug, panelistgender "
                 "FROM ww_panelists WHERE panelistslug != 'multiple' ORDER BY panelist ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        panelists = []
        for row in result:
            panelist = collections.OrderedDict()
            panelist["id"] = row["panelistid"]
            panelist["name"] = row["panelist"]
            panelist["slug"] = row["panelistslug"]
            panelist["gender"] = row["panelistgender"]
            panelists.append(panelist)

        return panelists
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def retrieve_all_ids(database_connection: mysql.connector.connect) -> List[int]:
    """Return a list of all panelist IDs, with IDs sorted in the order of panelist names.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[int]: List containing panelist IDs
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT panelistid FROM ww_panelists WHERE panelistslug != 'multiple' "
                 "ORDER BY panelist ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        panelists = []
        for row in result:
            panelists.append(row["panelistid"])

        return panelists
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def retrieve_by_id(panelist_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with panelist details based on the panelist ID.

    Arguments:
        panelist_id (int): Panelist ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the panelist ID has been validated or not
    Returns:
        OrderedDict: Returns an OrderedDict containing panelist id, name, gender
        and slug string
    """
    if not pre_validated_id:
        valid_id = validate_id(panelist_id, database_connection)
        if not valid_id:
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT panelist, panelistgender, panelistslug FROM ww_panelists "
                 "WHERE panelistid = %s;")

        cursor.execute(query, (panelist_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            panelist_dict = collections.OrderedDict()
            panelist_dict = {
                "id": panelist_id,
                "name": result["panelist"],
                "gender": result["panelistgender"],
                "slug": result["panelistslug"]
                }
            return panelist_dict

        return None
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def retrieve_by_slug(panelist_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with panelist details based on the panelist slug string

    Arguments:
        panelist_slug (str): Panelist slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        OrderedDict): Returns an OrderedDict containing panelist id, name, gender
        and slug string
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if panelist_id:
        return retrieve_by_id(panelist_id, database_connection, True)

    return None

def retrieve_appearances_by_id(panelist_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing information about all of the panelist's
    appearances.

    Arguments:
        panelist_id (int): Panelist ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the panelist ID has been validated or not
    Returns:
        list[OrderedDict]: Returns a list containing an OrderedDict with panelist
        appearance information
    """
    if not pre_validated_id:
        valid_id = validate_id(panelist_id, database_connection)
        if not valid_id:
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT ( "
                 "SELECT COUNT(pm.showid) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE s.bestof = 0 AND s.repeatshowid IS NULL AND "
                 "pm.panelistid = %s ) AS regular, ( "
                 "SELECT COUNT(pm.showid) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s ) AS allshows, ( "
                 "SELECT COUNT(pm.panelistid) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON pm.showid = s.showid "
                 "WHERE pm.panelistid = %s AND s.bestof = 0 AND "
                 "s.repeatshowid IS NULL AND pm.panelistscore IS NOT NULL ) "
                 "AS withscores;")
        cursor.execute(query, (panelist_id, panelist_id, panelist_id,))
        result = cursor.fetchone()
        cursor.close()

        appearance_counts = collections.OrderedDict()
        appearance_counts["regularShows"] = result["regular"]
        appearance_counts["allShows"] = result["allshows"]
        appearance_counts["showsWithScores"] = result["withscores"]

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.showid, s.showdate, s.bestof, s.repeatshowid, "
                 "pm.panelistlrndstart, pm.panelistlrndcorrect, pm.panelistscore,  "
                 "pm.showpnlrank FROM ww_showpnlmap pm "
                 "JOIN ww_panelists p ON p.panelistid = pm.panelistid "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "ORDER BY s.showdate ASC;")

        cursor.execute(query, (panelist_id,))
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
                appearance_info["lightningRoundStart"] = appearance["panelistlrndstart"]
                appearance_info["lightningRoundCorrect"] = appearance["panelistlrndcorrect"]
                appearance_info["score"] = appearance["panelistscore"]
                appearance_info["rank"] = bool(appearance["showpnlrank"])
                appearances.append(appearance_info)

            appearance_dict["count"] = appearance_counts
            appearance_dict["shows"] = appearances
        else:
            appearance_dict["count"] = 0
            appearance_dict["shows"] = None

        return appearance_dict
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

def retrieve_appearances_by_slug(panelist_slug: str,
                                 database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts containing information about all of the panelist's
    appearances.

    Arguments:
        panelist_slug (str): Panelist slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[OrderedDict]: Return a list containing an OrderedDict with panelist
        appearance information
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if panelist_id:
        return retrieve_appearances_by_id(panelist_id, database_connection, True)

    return None

def retrieve_statistics_by_id(panelist_id: int,
                              database_connection: mysql.connector.connect,
                              pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict containing panelist statistics, including ranking and
    scoring data.

    Arguments:
        panelist_id (int): Panelist ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the panelist ID has been validated or not
    Returns:
        OrderedDict: Returns an OrderedDict containing panelist statistics data
    """
    if not pre_validated_id:
        valid_id = validate_id(panelist_id, database_connection)
        if not valid_id:
            return None

    scores = []
    ranks = {}
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, pm.panelistscore FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE panelistid = %s "
                 "AND s.bestof = 0 and s.repeatshowid IS NULL;")
        cursor.execute(query, (panelist_id,))

        result = cursor.fetchall()
        cursor.close()

        for appearance in result:
            if appearance["panelistscore"]:
                scores.append(appearance["panelistscore"])

    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT ( "
                 "SELECT COUNT(pm.showpnlrank) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s AND pm.showpnlrank = '1' AND "
                 "s.bestof = 0 and s.repeatshowid IS NULL) as '1', ( "
                 "SELECT COUNT(pm.showpnlrank) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s AND pm.showpnlrank = '1t' AND "
                 "s.bestof = 0 and s.repeatshowid IS NULL) as '1t', ( "
                 "SELECT COUNT(pm.showpnlrank) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s AND pm.showpnlrank = '2' AND "
                 "s.bestof = 0 and s.repeatshowid IS NULL) as '2', ( "
                 "SELECT COUNT(pm.showpnlrank) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s AND pm.showpnlrank = '2t' AND "
                 "s.bestof = 0 and s.repeatshowid IS NULL) as '2t', ( "
                 "SELECT COUNT(pm.showpnlrank) FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s AND pm.showpnlrank = '3' AND "
                 "s.bestof = 0 and s.repeatshowid IS NULL "
                 ") as '3';")
        cursor.execute(query, (panelist_id, panelist_id, panelist_id, panelist_id, panelist_id,))

        result = cursor.fetchall()
        cursor.close()

        for appearance in result:
            ranks["first"] = appearance["1"]
            ranks["firstTied"] = appearance["1t"]
            ranks["second"] = appearance["2"]
            ranks["secondTied"] = appearance["2t"]
            ranks["third"] = appearance["3"]
    except mysql.connector.Error:
        raise Exception("Unable to query database: {}".format(mysql.connector.Error.with_traceback))

    statistics = collections.OrderedDict()
    scoring = collections.OrderedDict()
    ranking = collections.OrderedDict()

    appearance_count = len(scores)
    scoring["minimum"] = int(numpy.amin(scores))
    scoring["maximum"] = int(numpy.amax(scores))
    scoring["mean"] = round(numpy.mean(scores), 4)
    scoring["median"] = int(numpy.median(scores))
    scoring["standardDeviation"] = round(numpy.std(scores), 4)
    scoring["total"] = int(numpy.sum(scores))

    ranks_percentage = collections.OrderedDict()
    ranks_percentage["first"] = round(100 * (ranks["first"] / appearance_count), 4)
    ranks_percentage["firstTied"] = round(100 * (ranks["firstTied"] / appearance_count), 4)
    ranks_percentage["second"] = round(100 * (ranks["second"] / appearance_count), 4)
    ranks_percentage["secondTied"] = round(100 * (ranks["secondTied"] / appearance_count), 4)
    ranks_percentage["third"] = round(100 * (ranks["third"] / appearance_count), 4)

    ranking["rank"] = ranks
    ranking["percentage"] = ranks_percentage

    statistics["scoring"] = scoring
    statistics["ranking"] = ranking

    return statistics

def retrieve_statistics_by_slug(panelist_slug: str,
                                database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns an OrderedDict containing panelist statistics, including ranking and
    scoring data.

    Arguments:
        panelist_slug (str): Panelist slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        OrderedDict: Returns an OrderedDict containing panelist statistics data
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if panelist_id:
        return retrieve_statistics_by_id(panelist_id, database_connection, True)

    return None
