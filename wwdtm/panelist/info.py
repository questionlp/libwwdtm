# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving panelist information
from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from slugify import slugify
from wwdtm.panelist import utility

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
            panelist = OrderedDict()
            panelist["id"] = row["panelistid"]
            panelist["name"] = row["panelist"]
            if row["panelistslug"]:
                panelist["slug"] = row["panelistslug"]
            else:
                panelist["slug"] = slugify(panelist["name"])

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
        if not utility.validate_id(panelist_id, database_connection):
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
            panelist_dict = OrderedDict()
            panelist_dict["id"] = panelist_id
            panelist_dict["name"] = result["panelist"]
            if result["panelistslug"]:
                panelist_dict["slug"] = result["panelistslug"]
            else:
                panelist_dict["slug"] = slugify(panelist_dict["name"])

            panelist_dict["gender"] = result["panelistgender"]
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
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if panelist_id:
        return retrieve_by_id(panelist_id, database_connection, True)

    return None

def retrieve_scores_grouped_list_by_id(panelist_id: int,
                                       database_connection: mysql.connector.connect,
                                       pre_validated_id: bool = False
                                      ) -> Dict:
    """Returns an OrderedDict containing two lists, one with panelist
    scores and one with corresponding number of instances a panelist
    has scored that amount, for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT MIN(pm.panelistscore) AS min, "
                 "MAX(pm.panelistscore) AS max "
                 "FROM ww_showpnlmap pm;")
        cursor.execute(query)
        result = cursor.fetchone()

        if not result:
            return None

        min_score = result["min"]
        max_score = result["max"]

        scores = OrderedDict()
        for score in range(min_score, max_score + 1):
            scores[score] = 0

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.panelistscore AS score, "
                 "COUNT(pm.panelistscore) AS score_count "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "AND s.bestof = 0 AND s.repeatshowid IS NULL "
                 "AND pm.panelistscore IS NOT NULL "
                 "GROUP BY pm.panelistscore "
                 "ORDER BY pm.panelistscore ASC;")
        cursor.execute(query, (panelist_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        for row in result:
            scores[row["score"]] = row["score_count"]

        scores_list = OrderedDict()
        scores_list["score"] = list(scores.keys())
        scores_list["count"] = list(scores.values())
        return scores_list
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_scores_grouped_list_by_slug(panelist_slug: str,
                                         database_connection: mysql.connector.connect
                                        ) -> Dict:
    """Returns an OrderedDict containing two lists, one with panelist
    scores and one with corresponding number of instances a panelist
    has scored that amount, for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if not panelist_id:
        return None

    return retrieve_scores_grouped_list_by_id(panelist_id,
                                              database_connection,
                                              pre_validated_id=True)

def retrieve_scores_grouped_ordered_pair_by_id(panelist_id: int,
                                               database_connection: mysql.connector.connect,
                                               pre_validated_id: bool = False
                                              ) -> List[tuple]:
    """Returns an list of tuples containing a score and the
    corresponding number of instances a panelist has scored that amount
    for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT MIN(pm.panelistscore) AS min, "
                 "MAX(pm.panelistscore) AS max "
                 "FROM ww_showpnlmap pm;")
        cursor.execute(query)
        result = cursor.fetchone()

        if not result:
            return None

        min_score = result["min"]
        max_score = result["max"]

        scores = OrderedDict()
        for score in range(min_score, max_score + 1):
            scores[score] = 0

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.panelistscore AS score, "
                 "COUNT(pm.panelistscore) AS score_count "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "AND s.bestof = 0 AND s.repeatshowid IS NULL "
                 "AND pm.panelistscore IS NOT NULL "
                 "GROUP BY pm.panelistscore "
                 "ORDER BY pm.panelistscore ASC;")
        cursor.execute(query, (panelist_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        for row in result:
            scores[row["score"]] = row["score_count"]

        return list(scores.items())
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_scores_grouped_ordered_pair_by_slug(panelist_slug: str,
                                                 database_connection: mysql.connector.connect
                                                ) -> List[tuple]:
    """Returns an list of tuples containing a score and the
    corresponding number of instances a panelist has scored that amount
    for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if not panelist_id:
        return None

    return retrieve_scores_grouped_ordered_pair_by_id(panelist_id,
                                                      database_connection,
                                                      pre_validated_id=True)

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
        if not utility.validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, pm.panelistscore "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "AND s.bestof = 0 AND s.repeatshowid IS NULL "
                 "AND pm.panelistscore IS NOT NULL "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (panelist_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        show_list = []
        score_list = []
        for shows in result:
            show_list.append(shows["showdate"].isoformat())
            score_list.append(shows["panelistscore"])

        scores = OrderedDict()
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
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
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
        if not utility.validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showdate, pm.panelistscore "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "AND s.bestof = 0 AND s.repeatshowid IS NULL "
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
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if not panelist_id:
        return None

    return retrieve_scores_ordered_pair_by_id(panelist_id,
                                              database_connection,
                                              pre_validated_id=True)


def retrieve_yearly_appearances_by_id(panelist_id: int,
                                      database_connection: mysql.connector.connect,
                                      pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict containing a list of years and the
    corresponding number of appearances the panelist has made for the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated or not
    """
    if not pre_validated_id:
        if not utility.validate_id(panelist_id, database_connection):
            return None

    years = OrderedDict()
    cursor = database_connection.cursor(dictionary=True)
    query = ("SELECT DISTINCT YEAR(s.showdate) AS year FROM ww_shows s "
             "ORDER BY YEAR(s.showdate) ASC")
    cursor.execute(query)
    result = cursor.fetchall()

    if not result:
        return None

    for row in result:
        years[row["year"]] = 0

    cursor = database_connection.cursor(dictionary=True)
    query = ("SELECT YEAR(s.showdate) AS year, COUNT(p.panelist) AS count "
             "FROM ww_showpnlmap pm "
             "JOIN ww_shows s ON s.showid = pm.showid "
             "JOIN ww_panelists p ON p.panelistid = pm.panelistid "
             "WHERE pm.panelistid = %s AND s.bestof = 0 "
             "AND s.repeatshowid IS NULL "
             "GROUP BY p.panelist, YEAR(s.showdate) "
             "ORDER BY p.panelist ASC, YEAR(s.showdate) ASC")
    cursor.execute(query, (panelist_id, ))
    result = cursor.fetchall()

    if not result:
        return None

    for row in result:
        years[row["year"]] = row["count"]

    return years

def retrieve_yearly_appearances_by_slug(panelist_slug: str,
                                        database_connection: mysql.connector.connect
                                       ) -> Dict:
    """Returns an OrderedDict containing a list of years and the
    corresponding number of appearances the panelist has made for the
    requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """

    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if panelist_id:
        return retrieve_yearly_appearances_by_id(panelist_id,
                                                 database_connection,
                                                 True)

    return None

#endregion
