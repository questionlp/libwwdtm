# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving show details from the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
import datetime
from typing import List, Dict
import dateutil.parser as parser
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.show import core, info, utility

#region Show Details Retrieval Functions
def retrieve_by_id(show_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDicts with show details for the requested show
    ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the show ID has
        been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(show_id, database_connection):
            return None

    # Pull in the base show data, including date, host, scorekeeeper and notes
    show_info = core.retrieve_core_info_by_id(show_id, database_connection)
    if show_info:
        show_panelists = core.retrieve_panelist_info_by_id(show_id,
                                                           database_connection)
        show_bluff = core.retrieve_bluff_info_by_id(show_id,
                                                    database_connection)
        show_guests = core.retrieve_guest_info_by_id(show_id,
                                                     database_connection)

        show_details = OrderedDict()
        show_details["id"] = show_info["id"]
        show_details["date"] = show_info["date"]
        show_details["best_of"] = show_info["best_of"]
        show_details["repeat_show"] = show_info["repeat_show"]

        if "original_show_date" in show_info:
            show_details["original_show_id"] = show_info["original_show_id"]
            show_details["original_show_date"] = show_info["original_show_date"]

        show_details["location"] = show_info["location"]
        show_details["description"] = show_info["description"]
        show_details["notes"] = show_info["notes"]
        show_details["host"] = show_info["host"]
        show_details["scorekeeper"] = show_info["scorekeeper"]
        show_details["panelists"] = show_panelists
        show_details["bluff"] = show_bluff
        show_details["guests"] = show_guests
        return show_details

    return None

def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with show details for all shows

    Arguments:
        database_connection (mysql.connector.connect)
    """
    show_ids = info.retrieve_all_ids(database_connection)
    if not show_ids:
        return None

    shows = []
    for show_id in show_ids:
        show_detail = retrieve_by_id(show_id,
                                     database_connection,
                                     pre_validated_id=True)

        if show_detail:
            shows.append(show_detail)

    return shows

def retrieve_by_date(show_year: int,
                     show_month: int,
                     show_day: int,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDicts with show details for the requested show
    year, month and day

    Arguments:
        show_year (int): Four digit year required
        show_month (int)
        show_day (int)
        database_connection (mysql.connector.connect)
    """
    show_id = utility.convert_date_to_id(show_year,
                                         show_month,
                                         show_day,
                                         database_connection)
    if show_id:
        return retrieve_by_id(show_id, database_connection)

    return None

def retrieve_by_date_string(show_date: str,
                            database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDicts with show details for the requested show
    date string

    Arguments:
        show_date (str): Show date in YYYY-MM-DD format
        database_connection (mysql.connector.connect)
    """
    try:
        parsed_show_date = parser.parse(show_date)
    except ValueError:
        return None

    show_id = utility.convert_date_to_id(parsed_show_date.year,
                                         parsed_show_date.month,
                                         parsed_show_date.day,
                                         database_connection)
    if show_id:
        return retrieve_by_id(show_id, database_connection)

    return None

def retrieve_by_year(show_year: int,
                     database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with show details for the
    requested show year

    Arguments:
        show_year (int): Four digit year is required
        database_connection (mysql.connector.connect)
    """
    try:
        parsed_show_year = parser.parse("{}".format(show_year))
    except ValueError:
        return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows "
                 "WHERE YEAR(showdate) = %s "
                 "ORDER BY showdate ASC;")
        cursor.execute(query, (parsed_show_year.year,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        shows = []
        for show in result:
            show_detail = retrieve_by_id(show["showid"],
                                         database_connection,
                                         pre_validated_id=True)
            shows.append(show_detail)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_year_month(show_year: int,
                           show_month: int,
                           database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with show details for the
    requested show year and month

    Arguments:
        show_year (int): Four digit year is required
        show_month (int)
        database_connection (mysql.connector.connect)
    """
    try:
        parsed_show_year_month = parser.parse("{}-{}".format(show_year,
                                                             show_month))
    except ValueError:
        return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows "
                 "WHERE YEAR(showdate) = %s "
                 "AND MONTH(showdate) = %s "
                 "ORDER BY showdate ASC;")
        cursor.execute(query, (parsed_show_year_month.year,
                               parsed_show_year_month.month,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        shows = []
        for show in result:
            show_detail = retrieve_by_id(show["showid"],
                                         database_connection,
                                         pre_validated_id=True)
            shows.append(show_detail)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_recent(database_connection: mysql.connector.connect,
                    include_days_ahead: int = 7,
                    include_days_back: int = 32,) -> List[Dict]:
    """Returns a list of OrderedDicts with show details for recent
    shows

    Arguments:
        database_connection (mysql.connector.connect)
        include_days_ahead (int): Number of days in the future to
        include (default: 7)
        include_days_back (int): Number of days in the past to include
        (default: 32)
    """
    try:
        past_days = int(include_days_back)
        future_days = int(include_days_ahead)
    except ValueError:
        return None

    try:
        past_date = datetime.datetime.now() - datetime.timedelta(days=past_days)
        future_date = datetime.datetime.now() + datetime.timedelta(days=future_days)
    except OverflowError:
        return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows "
                 "WHERE showdate >= %s AND "
                 "showdate <= %s ORDER BY showdate ASC;")
        cursor.execute(query,
                       (past_date.isoformat(),
                        future_date.isoformat()))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        shows = []
        for show in result:
            show_detail = retrieve_by_id(show["showid"],
                                         database_connection,
                                         pre_validated_id=True)
            shows.append(show_detail)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
