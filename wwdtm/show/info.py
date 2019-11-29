# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving show information from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
import datetime
from typing import List, Dict
import dateutil.parser as parser
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.show import utility

#region Show Basic Info Retrieval Functions
def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> List[int]:
    """Returns a list of all show IDs, sorted by show date

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT showid FROM ww_shows ORDER BY showdate ASC;"
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        show_ids = []
        for show in result:
            show_ids.append(show[0])

        return show_ids
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_dates(database_connection: mysql.connector.connect
                      ) -> List[str]:
    """Returns a list of all show dates, sorted by show date

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT showdate FROM ww_shows ORDER BY showdate ASC;"
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        show_dates = []
        for show in result:
            show_dates.append(show[0].isoformat())

        return show_dates
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_dates_tuple(database_connection: mysql.connector.connect
                            ) -> List[tuple]:
    """Returns a list of all show dates as a tuple of year, month and
    day, sorted by show date

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT YEAR(showdate), MONTH(showdate), DAY(showdate) "
                 "FROM ww_shows "
                 "ORDER BY showdate ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        show_dates = []
        for show in result:
            show_dates.append((show[0], show[1], show[2]))

        return show_dates
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_show_years_months(database_connection: mysql.connector.connect
                                  ) -> List[str]:
    """Returns a list of all show years and months as a string, sorted
    by year and month

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT DISTINCT YEAR(showdate), MONTH(showdate) "
                 "FROM ww_shows "
                 "ORDER BY showdate ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        show_years_months = []
        for row in result:
            year_month = "{}-{}".format(row[0], row[1])
            show_years_months.append(year_month)

        return show_years_months
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_show_years_months_tuple(database_connection: mysql.connector.connect
                                        ) -> List[tuple]:
    """Returns a list of all show years and months as a tuple, sorted
    by year and month

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT DISTINCT YEAR(showdate), MONTH(showdate) "
                 "FROM ww_shows "
                 "ORDER BY showdate ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        show_years_months = []
        for row in result:
            show_years_months.append((row[0], row[1]))

        return show_years_months
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_id(show_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with show information for the requested
    show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the show ID has
        been validated
    """

    if not pre_validated_id:
        if not utility.validate_id(show_id, database_connection):
            return None

    try:
        # Pull in base show information, including: show ID, date,
        # Best Of flag and, if applicable, the show ID of the original
        # show if it is a repeat
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid, showdate, bestof, repeatshowid "
                 "FROM ww_shows "
                 "WHERE showid = %s;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        repeat_show_id = result["repeatshowid"]
        show_info = OrderedDict()
        show_info["id"] = show_id
        show_info["date"] = result["showdate"].isoformat()
        show_info["best_of"] = bool(result["bestof"])
        show_info["repeat_show"] = bool(repeat_show_id)

        if repeat_show_id:
            show_info["original_show_id"] = repeat_show_id
            show_info["original_show_date"] = utility.convert_id_to_date(repeat_show_id,
                                                                         database_connection)

        return show_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with show information for all
    shows

    Arguments:
        database_connection (mysql.connector.connect)
    """
    show_ids = retrieve_all_ids(database_connection)
    if not show_ids:
        return None

    shows = []
    for show_id in show_ids:
        show_info = retrieve_by_id(show_id, database_connection, True)

        if show_info:
            shows.append(show_info)

    return shows

def retrieve_by_date(show_year: int,
                     show_month: int,
                     show_day: int,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with show information based on the
    show's year, month and day

    Arguments:
        show_year (int): Four digit year is required
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
    """Returns an OrderedDict with show information based on the show's
    date string

    Arguments:
        show_date (str): Show date in YYYY-MM-DD format
        database_connection (mysql.connector.connect)
    """
    try:
        parsed_show_date = parser.parse(show_date)
    except ValueError as err:
        raise ValueError("Invalid date string") from err

    show_id = utility.convert_date_to_id(parsed_show_date.year,
                                         parsed_show_date.month,
                                         parsed_show_date.day,
                                         database_connection)
    if show_id:
        return retrieve_by_id(show_id, database_connection)

    return None

def retrieve_months_by_year(show_year: int,
                            database_connection: mysql.connector.connect
                           ) -> List[int]:
    """Returns a list of show months available for the requested year

    Arguments:
        show_year (int): Four digit year is required
        database_connection (mysql.connector.connect)
    """
    try:
        _ = parser.parse("{:04d}".format(show_year))
    except ValueError as err:
        raise ValueError("Invalid year value") from err

    try:
        cursor = database_connection.cursor()
        query = ("SELECT DISTINCT MONTH(showdate) "
                 "FROM ww_shows "
                 "WHERE YEAR(showdate) = %s "
                 "ORDER BY MONTH(showdate) ASC;")
        cursor.execute(query, (show_year,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        months = []
        for row in result:
            months.append(row[0])

        return months
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_years(database_connection: mysql.connector.connect) -> List[int]:
    """Returns list of available show years

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT DISTINCT YEAR(showdate) "
                 "FROM ww_shows "
                 "ORDER BY YEAR(showdate) ASC;")
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        years = []
        for row in result:
            years.append(row[0])

        return years
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_year(show_year: int,
                     database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with show information for the
    requested show year

    Arguments:
        show_year (int): Four digit year is required
        database_connection (mysql.connector.connect)
    """
    try:
        parsed_show_year = parser.parse("{:04d}".format(show_year))
    except ValueError as err:
        raise ValueError("Invalid year value") from err

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
            show_info = retrieve_by_id(show["showid"],
                                       database_connection,
                                       pre_validated_id=True)
            shows.append(show_info)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_year_month(show_year: int,
                           show_month: int,
                           database_connection: mysql.connector.connect
                          ) -> List[Dict]:
    """Returns a list of OrderedDicts with show information for the
    requested show year and month

    Arguments:
        show_year (int): Four digit year is required
        show_month (int)
        database_connection (mysql.connector.connect)
    """
    try:
        # Validate year/month by trying to parse a date string for
        # the first day of the year/month
        parsed_show_year_month = parser.parse("{:04d}-{:02d}-01".format(show_year,
                                                                        show_month))
    except ValueError as err:
        raise ValueError("Invalid year and month value") from err

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows "
                 "WHERE YEAR(showdate) = %s "
                 "AND MONTH(showdate) = %s ORDER BY showdate ASC;")
        cursor.execute(query, (parsed_show_year_month.year,
                               parsed_show_year_month.month,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        shows = []
        for show in result:
            show_info = retrieve_by_id(show["showid"],
                                       database_connection,
                                       pre_validated_id=True)
            shows.append(show_info)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_recent(database_connection: mysql.connector.connect,
                    include_days_ahead: int = 7,
                    include_days_back: int = 32,) -> List[Dict]:
    """Returns a list of OrderedDicts with show information for recent
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
            show_info = retrieve_by_id(show["showid"],
                                       database_connection,
                                       pre_validated_id=True)
            shows.append(show_info)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
