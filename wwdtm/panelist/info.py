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
from wwdtm.panelist import core, utility

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
            panelist = OrderedDict(id=row["panelistid"],
                                   name=row["panelist"],
                                   slug=row["panelistslug"],
                                   gender=row["panelistgender"])
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
            panelist_dict = OrderedDict(id=panelist_id,
                                        name=result["panelist"],
                                        slug=result["panelistslug"],
                                        gender=result["panelistgender"])
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
        if not utility.validate_id(panelist_id, database_connection):
            return None

    panelist = retrieve_by_id(panelist_id,
                              database_connection,
                              pre_validated_id=True)
    panelist["statistics"] = core.retrieve_statistics_by_id(panelist_id,
                                                            database_connection,
                                                            pre_validated_id=True)
    panelist["appearances"] = core.retrieve_appearances_by_id(panelist_id,
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
    panelist_id = utility.convert_slug_to_id(panelist_id, database_connection)
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
        if not utility.validate_id(panelist_id, database_connection):
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

        show_list = []
        score_list = []
        for shows in result:
            show_list.append(shows["showdate"].isoformat())
            score_list.append(shows["panelistscore"])

        return OrderedDict(shows=show_list, scores=score_list)
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
    panelist_id = utility.convert_slug_to_id(panelist_slug,
                                             database_connection)
    if not panelist_id:
        return None

    return retrieve_scores_ordered_pair_by_id(panelist_id,
                                              database_connection,
                                              pre_validated_id=True)

#endregion
