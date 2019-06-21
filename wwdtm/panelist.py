# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_panelists table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
import numpy

#region Internal Functions
def _retrieve_appearances_by_id(panelist_id: int,
                                database_connection: mysql.connector.connect,
                                pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated or not
    """
    if not pre_validated_id:
        if not validate_id(panelist_id, database_connection):
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
                 "s.repeatshowid IS NULL "
                 "AND pm.panelistscore IS NOT NULL ) "
                 "AS withscores;")
        cursor.execute(query, (panelist_id, panelist_id, panelist_id,))
        result = cursor.fetchone()
        cursor.close()

        appearance_counts = collections.OrderedDict()
        appearance_counts["regularShows"] = result["regular"]
        appearance_counts["allShows"] = result["allshows"]
        appearance_counts["showsWithScores"] = result["withscores"]

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.showid, s.showdate, s.bestof, "
                 "s.repeatshowid, pm.panelistlrndstart, "
                 " pm.panelistlrndcorrect, pm.panelistscore,  "
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
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_appearances_by_slug(panelist_slug: str,
                                  database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if panelist_id:
        return _retrieve_appearances_by_id(panelist_id, database_connection, True)

    return None

def _retrieve_scores_by_id(panelist_id: int,
                           database_connection: mysql.connector.connect
                          ) -> List[int]:
    """Returns a list of panelist scores for appearances for the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
    scores = []
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, pm.panelistscore "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE panelistid = %s "
                 "AND s.bestof = 0 and s.repeatshowid IS NULL;")
        cursor.execute(query, (panelist_id,))

        result = cursor.fetchall()
        cursor.close()

        for appearance in result:
            if appearance["panelistscore"]:
                scores.append(appearance["panelistscore"])

        return scores
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_rank_info_by_id(panelist_id: int,
                              database_connection: mysql.connector.connect
                             ) -> Dict:
    """Returns an OrderedDict with ranking information for the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        ranks = collections.OrderedDict()
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
        cursor.execute(query, (panelist_id,
                               panelist_id,
                               panelist_id,
                               panelist_id,
                               panelist_id,))

        result = cursor.fetchall()
        cursor.close()

        for appearance in result:
            ranks["first"] = appearance["1"]
            ranks["firstTied"] = appearance["1t"]
            ranks["second"] = appearance["2"]
            ranks["secondTied"] = appearance["2t"]
            ranks["third"] = appearance["3"]

        return ranks
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_statistics_by_id(panelist_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict containing panelist statistics, ranking
    data, and scoring data for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated or not
    """
    if not pre_validated_id:
        if not validate_id(panelist_id, database_connection):
            return None

    scores = _retrieve_scores_by_id(panelist_id, database_connection)
    ranks = _retrieve_rank_info_by_id(panelist_id, database_connection)
    if not scores or not ranks:
        return None

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

def _retrieve_statistics_by_slug(panelist_slug: str,
                                 database_connection: mysql.connector.connect
                                ) -> List[Dict]:
    """Returns a list of OrderedDicts containing panelist statistics,
    ranking data, and scoring data for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if panelist_id:
        return _retrieve_statistics_by_id(panelist_id, database_connection, True)

    return None

#endregion

#region Utility Functions
def convert_slug_to_id(panelist_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a panelist's slug based on the requested panelist ID

    Arguments:
        panelist_slug (str)
        database_connect (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistid FROM ww_panelists "
                 "WHERE panelistslug = %s;")
        cursor.execute(query, (panelist_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_id(panelist_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist ID is
    valid

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        panelist_id = int(panelist_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistid FROM ww_panelists "
                 "WHERE panelistid = %s;")
        cursor.execute(query, (panelist_id,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_slug(panelist_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist slug is
    valid

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_slug = panelist_slug.strip()
    if not panelist_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistslug FROM ww_panelists "
                 "WHERE panelistslug = %s;")
        cursor.execute(query, (panelist_slug,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(panelist_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist ID
    exists

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(panelist_id, database_connection)

def slug_exists(panelist_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a panelist slug
    exists

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(panelist_slug, database_connection)

#endregion

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts containing panelist details for
    all panelists

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT panelistid, panelist, panelistslug, "
                 "panelistgender "
                 "FROM ww_panelists "
                 "WHERE panelistslug != 'multiple' "
                 "ORDER BY panelist ASC;")
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
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> List[int]:
    """Return a list of all panelist IDs, sorted by panelist names

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT panelistid FROM ww_panelists "
                 "WHERE panelistslug != 'multiple' "
                 "ORDER BY panelist ASC;")
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

def retrieve_by_id(panelist_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with panelist information based on the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT panelist, panelistgender, panelistslug "
                 "FROM ww_panelists "
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
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_slug(panelist_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with panelist information based on the
    requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if panelist_id:
        return retrieve_by_id(panelist_id, database_connection, True)

    return None

def retrieve_details_by_id(panelist_id: int,
                           database_connection: mysql.connector.connect,
                           pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with panelist details based on the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(panelist_id, database_connection):
            return None

    panelist = retrieve_by_id(panelist_id,
                              database_connection,
                              pre_validated_id=True)
    panelist["statistics"] = _retrieve_statistics_by_id(panelist_id,
                                                        database_connection,
                                                        pre_validated_id=True)
    panelist["appearances"] = _retrieve_appearances_by_id(panelist_id,
                                                          database_connection,
                                                          pre_validated_id=True)
    return panelist

def retrieve_details_by_slug(panelist_id: int,
                             database_connection: mysql.connector.connect
                            ) -> Dict:
    """Returns an OrderedDict with panelist details based on the
    requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = convert_slug_to_id(panelist_id, database_connection)
    if not panelist_id:
        return None

    return retrieve_details_by_id(panelist_id,
                                  database_connection,
                                  pre_validated_id=True)

def retrieve_all_details(database_connection: mysql.connector.connect
                        ) -> List[Dict]:
    """Returns a list of OrderedDicts with panelist details for all
    panelists

    Arguments:
        database_connection (mysql.connector.connect)
    """
    panelist_ids = retrieve_all_ids(database_connection)
    if not panelist_ids:
        return None

    panelists = []
    for panelist_id in panelist_ids:
        panelist = retrieve_details_by_id(panelist_id,
                                          database_connection,
                                          pre_validated_id=True)
        if panelist:
            panelists.append(panelist)

    return panelists

def retrieve_scores_list_by_id(panelist_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict containing two lists, one with show dates
    and one with corresponding scores for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, pm.panelistscore "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "AND s.bestof = 0 and s.repeatshowid IS NULL "
                 "AND pm.panelistscore IS NOT NULL "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (panelist_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        scores = collections.OrderedDict()
        show_list = []
        score_list = []
        for shows in result:
            show_list.append(shows["showdate"].isoformat())
            score_list.append(shows["panelistscore"])

        scores["shows"] = show_list
        scores["scores"] = score_list
        return scores
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_scores_list_by_slug(panelist_slug: str,
                                 database_connection: mysql.connector.connect
                                ) -> Dict:
    """Returns an OrderedDict containing two lists, one with show dates
    and one with corresponding scores for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if not panelist_id:
        return None

    return retrieve_scores_list_by_id(panelist_id,
                                      database_connection,
                                      pre_validated_id=True)

def retrieve_scores_ordered_pair_by_id(panelist_id: int,
                                       database_connection: mysql.connector.connect,
                                       pre_validated_id: bool = False
                                      ) -> List[tuple]:
    """Returns an list of tuples containing a show date and the
    corresponding score for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, pm.panelistscore "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "AND s.bestof = 0 and s.repeatshowid IS NULL "
                 "AND pm.panelistscore IS NOT NULL "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (panelist_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        scores = []
        for show in result:
            show_date = show["showdate"].isoformat()
            score = show["panelistscore"]
            scores.append((show_date, score))

        return scores
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_scores_ordered_pair_by_slug(panelist_slug: str,
                                         database_connection: mysql.connector.connect
                                        ) -> List[tuple]:
    """Returns an list of tuples containing a show date and the
    corresponding score for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = convert_slug_to_id(panelist_slug, database_connection)
    if not panelist_id:
        return None

    return retrieve_scores_ordered_pair_by_id(panelist_id,
                                              database_connection,
                                              pre_validated_id=True)

#endregion
