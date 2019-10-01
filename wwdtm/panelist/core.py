# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides core functions for retrieving panelist
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
import numpy
from wwdtm.panelist import utility

#region Internal Functions
def retrieve_appearances_by_id(panelist_id: int,
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
        if not utility.validate_id(panelist_id, database_connection):
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

        appearance_counts = OrderedDict(regular_shows=result["regular"],
                                        all_shows=result["allshows"],
                                        shows_with_scores=result["withscores"])

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.showid, s.showdate, s.bestof, "
                 "s.repeatshowid, pm.panelistlrndstart AS start, "
                 " pm.panelistlrndcorrect AS correct, pm.panelistscore, "
                 "pm.showpnlrank FROM ww_showpnlmap pm "
                 "JOIN ww_panelists p ON p.panelistid = pm.panelistid "
                 "JOIN ww_shows s ON s.showid = pm.showid "
                 "WHERE pm.panelistid = %s "
                 "ORDER BY s.showdate ASC;")

        cursor.execute(query, (panelist_id,))
        result = cursor.fetchall()
        cursor.close()

        if result:
            appearances = []
            for appearance in result:
                rank = appearance["showpnlrank"]
                if not rank:
                    rank = None

                appearance_info = OrderedDict(date=appearance["showdate"].isoformat(),
                                              best_of=bool(appearance["bestof"]),
                                              repeat_show=bool(appearance["repeatshowid"]),
                                              lightning_round_start=appearance["start"],
                                              lightning_round_correct=appearance["correct"],
                                              score=appearance["panelistscore"],
                                              rank=rank)
                appearances.append(appearance_info)

            return OrderedDict(count=appearance_counts, shows=appearances)

        return OrderedDict(count=0, shows=None)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_appearances_by_slug(panelist_slug: str,
                                 database_connection: mysql.connector.connect
                                ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if panelist_id:
        return retrieve_appearances_by_id(panelist_id, database_connection, True)

    return None

def retrieve_bluffs_by_id(panelist_id: int,
                          database_connection: mysql.connector.connect,
                          pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict containing Bluff the Listener information
    for the requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the panelist ID
        has been validated or not
    """
    if not pre_validated_id:
        if not utility.validate_id(panelist_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor()
        query = ("SELECT ( "
                 "SELECT COUNT(blm.chosenbluffpnlid) FROM ww_showbluffmap blm "
                 "JOIN ww_shows s ON s.showid = blm.showid "
                 "WHERE s.repeatshowid IS NULL AND blm.chosenbluffpnlid = %s "
                 ") AS chosen, ( "
                 "SELECT COUNT(blm.correctbluffpnlid) FROM ww_showbluffmap blm "
                 "JOIN ww_shows s ON s.showid = blm.showid "
                 "WHERE s.repeatshowid IS NULL AND blm.correctbluffpnlid = %s "
                 ") AS correct;")
        cursor.execute(query, (panelist_id, panelist_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return OrderedDict(chosen=result[0], correct=result[1])

        return None

    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_bluffs_by_slug(panelist_slug: str,
                            database_connection: mysql.connector.connect
                           ) -> Dict:
    """Returns an OrderedDict containing Bluff the Listener information
    for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if panelist_id:
        return retrieve_bluffs_by_id(panelist_id, database_connection, True)

    return None

def retrieve_scores_by_id(panelist_id: int,
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

def retrieve_rank_info_by_id(panelist_id: int,
                             database_connection: mysql.connector.connect
                            ) -> Dict:
    """Returns an OrderedDict with ranking information for the
    requested panelist ID

    Arguments:
        panelist_id (int)
        database_connection (mysql.connector.connect)
    """
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
        cursor.execute(query, (panelist_id,
                               panelist_id,
                               panelist_id,
                               panelist_id,
                               panelist_id,))
        result = cursor.fetchone()
        cursor.close()

        return OrderedDict(first=result["1"],
                           first_tied=result["1t"],
                           second=result["2"],
                           second_tied=result["2t"],
                           third=result["3"])
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_statistics_by_id(panelist_id: int,
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
        if not utility.validate_id(panelist_id, database_connection):
            return None

    scores = retrieve_scores_by_id(panelist_id, database_connection)
    ranks = retrieve_rank_info_by_id(panelist_id, database_connection)
    if not scores or not ranks:
        return None

    appearance_count = len(scores)
    scoring = OrderedDict(minimum=int(numpy.amin(scores)),
                          maximum=int(numpy.amax(scores)),
                          mean=round(numpy.mean(scores), 4),
                          median=int(numpy.median(scores)),
                          standard_deviation=round(numpy.std(scores), 4),
                          total=int(numpy.sum(scores)))

    ranks_first = round(100 * (ranks["first"] / appearance_count), 4)
    ranks_first_tied = round(100 * (ranks["first_tied"] / appearance_count), 4)
    ranks_second = round(100 * (ranks["second"] / appearance_count), 4)
    ranks_second_tied = round(100 * (ranks["second_tied"] / appearance_count), 4)
    ranks_third = round(100 * (ranks["third"] / appearance_count), 4)
    ranks_percentage = OrderedDict(first=ranks_first,
                                   first_tied=ranks_first_tied,
                                   second=ranks_second,
                                   second_tied=ranks_second_tied,
                                   third=ranks_third)

    ranking = OrderedDict(rank=ranks, percentage=ranks_percentage)
    return OrderedDict(scoring=scoring, ranking=ranking)

def retrieve_statistics_by_slug(panelist_slug: str,
                                database_connection: mysql.connector.connect
                               ) -> List[Dict]:
    """Returns a list of OrderedDicts containing panelist statistics,
    ranking data, and scoring data for the requested panelist slug

    Arguments:
        panelist_slug (str)
        database_connection (mysql.connector.connect)
    """
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if panelist_id:
        return retrieve_statistics_by_id(panelist_id, database_connection, True)

    return None

#endregion
