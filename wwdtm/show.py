# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_shows table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
import datetime
from typing import List, Dict
import dateutil.parser as parser
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_core_info_by_id(show_id: int,
                              database_connection: mysql.connector.connect
                             ) -> Dict:
    """Returns an OrderedDict with core information for the requested
    show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        show_info = collections.OrderedDict()
        location_info = collections.OrderedDict()

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showid, s.showdate, s.bestof, "
                 "s.repeatshowid, l.city, l.state, l.venue, "
                 "h.hostid, h.host, h.hostslug, hm.guest as hostguest, "
                 "sk.scorekeeperid, sk.scorekeeper, sk.scorekeeperslug, "
                 "skm.guest AS scorekeeperguest, skm.description, "
                 "sd.showdescription, sn.shownotes "
                 "FROM ww_shows s "
                 "JOIN ww_showlocationmap lm ON lm.showid = s.showid "
                 "JOIN ww_locations l ON l.locationid = lm.locationid "
                 "JOIN ww_showhostmap hm ON hm.showid = s.showid "
                 "JOIN ww_hosts h ON h.hostid = hm.hostid "
                 "JOIN ww_showskmap skm ON skm.showid = s.showid "
                 "JOIN ww_scorekeepers sk ON "
                 "sk.scorekeeperid = skm.scorekeeperid "
                 "JOIN ww_showdescriptions sd ON sd.showid = s.showid "
                 "JOIN ww_shownotes sn ON sn.showid = s.showid "
                 "WHERE s.showid = %s;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None

        show_info["id"] = show_id
        show_info["date"] = result["showdate"].isoformat()
        show_info["bestOf"] = bool(result["bestof"])
        if result["repeatshowid"]:
            show_info["isRepeat"] = True
            original_show_date = convert_id_to_date(result["repeatshowid"],
                                                    database_connection)
            show_info["originalShowDate"] = original_show_date.isoformat()
        else:
            show_info["isRepeat"] = False

        location_info["city"] = result["city"]
        location_info["state"] = result["state"]
        location_info["venue"] = result["venue"]
        show_info["location"] = location_info

        show_info["description"] = str(result["showdescription"]).strip()
        if result["shownotes"]:
            show_info["notes"] = str(result["shownotes"]).strip()
        else:
            show_info["notes"] = None

        show_host = collections.OrderedDict()
        show_host["id"] = result["hostid"]
        show_host["name"] = result["host"]
        show_host["slug"] = result["hostslug"]
        show_host["guest"] = bool(result["hostguest"])
        show_info["host"] = show_host

        show_scorekeeper = collections.OrderedDict()
        show_scorekeeper["id"] = result["scorekeeperid"]
        show_scorekeeper["name"] = result["scorekeeper"]
        show_scorekeeper["slug"] = result["scorekeeperslug"]
        show_scorekeeper["guest"] = bool(result["scorekeeperguest"])
        show_scorekeeper["description"] = result["description"]
        show_info["scorekeeper"] = show_scorekeeper

        return show_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_panelist_info_by_id(show_id: int,
                                  database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing panelist information
    for the requested show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.panelistid, p.panelist, p.panelistslug, "
                 "pm.panelistlrndstart, pm.panelistlrndcorrect, "
                 "pm.panelistscore, pm.showpnlrank "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_panelists p on p.panelistid = pm.panelistid "
                 "WHERE pm.showid = %s "
                 "ORDER by pm.panelistscore DESC, pm.showpnlmapid ASC;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        panelists = []
        for panelist in result:
            panelist_info = collections.OrderedDict()
            panelist_info["id"] = panelist["panelistid"]
            panelist_info["name"] = panelist["panelist"]
            panelist_info["slug"] = panelist["panelistslug"]
            panelist_info["lightningRoundStart"] = panelist["panelistlrndstart"]
            panelist_info["lightningRoundCorrect"] = panelist["panelistlrndcorrect"]
            panelist_info["score"] = panelist["panelistscore"]
            panelist_info["rank"] = panelist["showpnlrank"]
            panelists.append(panelist_info)

        return panelists
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_bluff_info_by_id(show_id: int,
                               database_connection: mysql.connector.connect
                              ) -> Dict:
    """Returns an OrderedDicts containing panelist bluff information
    for the requested show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        bluff_info = collections.OrderedDict()
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT blm.chosenbluffpnlid, p.panelist, "
                 "p.panelistslug "
                 "FROM ww_showbluffmap blm "
                 "JOIN ww_shows s ON s.showid = blm.showid "
                 "JOIN ww_panelists p ON "
                 "p.panelistid = blm.chosenbluffpnlid "
                 "WHERE s.showid = %s;")
        cursor.execute(query, (show_id,))
        chosen_result = cursor.fetchone()

        chosen_bluff_info = collections.OrderedDict()
        if chosen_result:
            chosen_bluff_info["id"] = chosen_result["chosenbluffpnlid"]
            chosen_bluff_info["name"] = chosen_result["panelist"]
            chosen_bluff_info["slug"] = chosen_result["panelistslug"]
        else:
            chosen_bluff_info = None

        query = ("SELECT blm.correctbluffpnlid, p.panelist, "
                 "p.panelistslug "
                 "FROM ww_showbluffmap blm "
                 "JOIN ww_shows s ON s.showid = blm.showid "
                 "JOIN ww_panelists p ON "
                 "p.panelistid = blm.correctbluffpnlid "
                 "WHERE s.showid = %s;")
        cursor.execute(query, (show_id,))
        correct_result = cursor.fetchone()
        cursor.close()

        correct_bluff_info = collections.OrderedDict()
        if correct_result:
            correct_bluff_info["id"] = correct_result["correctbluffpnlid"]
            correct_bluff_info["name"] = correct_result["panelist"]
            correct_bluff_info["slug"] = correct_result["panelistslug"]
        else:
            correct_bluff_info = None

        bluff_info["chosenPanelist"] = chosen_bluff_info
        bluff_info["correctPanelist"] = correct_bluff_info

        return bluff_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_not_my_job_info_by_id(show_id: int,
                                    database_connection: mysql.connector.connect
                                   ) -> List[Dict]:
    """Returns a list of OrderedDicts containing Not My Job information
    for the requested show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT gm.guestid, g.guest, g.guestslug, "
                 "gm.guestscore, gm.exception "
                 "FROM ww_showguestmap gm "
                 "JOIN ww_guests g on g.guestid = gm.guestid "
                 "JOIN ww_shows s on s.showid = gm.showid "
                 "WHERE gm.showid = %s "
                 "ORDER by gm.showguestmapid ASC;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            return None

        guests = []
        for guest in result:
            guest_info = collections.OrderedDict()
            guest_info["id"] = guest["guestid"]
            guest_info["name"] = guest["guest"]
            guest_info["slug"] = guest["guestslug"]
            guest_info["score"] = guest["guestscore"]
            guest_info["scoreException"] = bool(guest["exception"])
            guests.append(guest_info)

        return guests
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion

#region Utility Functions
def validate_id(show_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on wheter or not a show ID is valid

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        show_id = int(show_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT showid from ww_shows where showid = %s;"
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def convert_date_to_id(show_year: int,
                       show_month: int,
                       show_day: int,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a show's ID based on the show's year, month and day

    Arguments:
        show_year (int): Four digit year is required
        show_month (int)
        show_day (int)
        database_connection (mysql.connector.connect)
    """
    show_date = None
    try:
        show_date = datetime.datetime(year=show_year,
                                      month=show_month,
                                      day=show_day)
    except ValueError as err:
        raise ValueError("Invalid year, month and/or day value") from err

    try:
        show_date_str = show_date.isoformat()
        cursor = database_connection.cursor()
        query = "SELECT showid from ww_shows WHERE showdate = %s;"
        cursor.execute(query, (show_date_str,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def convert_id_to_date(show_id: int,
                       database_connection: mysql.connector.connect
                      ) -> datetime.datetime:
    """Returns a show's date based on the show's ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT showdate FROM ww_shows WHERE showid = %s;"
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(show_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a show ID exists

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(show_id, database_connection)

def date_exists(show_year: int,
                show_month: int,
                show_day: int,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or false based on whether or not a show exists for
    the requested year, month and day

    Arguments:
        show_year (int): Four digit year is required
        show_month (int)
        show_day (int)
        database_connection (mysql.connector.connect)
    """
    show_date = None
    try:
        show_date = datetime.datetime(show_year, show_month, show_day)
    except ValueError as err:
        raise ValueError("Invalid year, month and/or day value") from err

    try:
        show_date_str = show_date.isoformat()
        cursor = database_connection.cursor()
        query = "SELECT showid from ww_shows WHERE showdate = %s;"
        cursor.execute(query, (show_date_str,))
        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion

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
        if not validate_id(show_id, database_connection):
            return None

    try:
        show_info = collections.OrderedDict()

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

        show_info["id"] = result["showid"]
        show_info["date"] = result["showdate"].isoformat()
        show_info["bestOf"] = bool(result["bestof"])
        if result["repeatshowid"]:
            show_info["isRepeat"] = True
            original_show_date = convert_id_to_date(result["repeatshowid"],
                                                    database_connection)
            show_info["originalShowDate"] = original_show_date.isoformat()
        else:
            show_info["isRepeat"] = False
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
    show_id = convert_date_to_id(show_year,
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

    show_id = convert_date_to_id(parsed_show_date.year,
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
        _ = parser.parse("{}".format(show_year))
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
        parsed_show_year = parser.parse("{}".format(show_year))
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
        parsed_show_year_month = parser.parse("{}-{}".format(show_year,
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

#region Show Details Retrieval Functions
def retrieve_details_by_id(show_id: int,
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
        if not validate_id(show_id, database_connection):
            return None

    show_details = collections.OrderedDict()

    # Pull in the base show data, including date, host, scorekeeeper and notes
    show_info = _retrieve_core_info_by_id(show_id, database_connection)
    if show_info:
        show_details["id"] = show_info["id"]
        show_details["date"] = show_info["date"]
        show_details["bestOf"] = show_info["bestOf"]
        show_details["isRepeat"] = show_info["isRepeat"]
        if show_details["isRepeat"]:
            show_details["originalShowDate"] = show_info["originalShowDate"]
        show_details["location"] = show_info["location"]
        show_details["description"] = show_info["description"]
        show_details["notes"] = show_info["notes"]
        show_details["host"] = show_info["host"]
        show_details["scorekeeper"] = show_info["scorekeeper"]
        show_details["panelists"] = _retrieve_panelist_info_by_id(show_id,
                                                                  database_connection)
        show_details["bluff"] = _retrieve_bluff_info_by_id(show_id,
                                                           database_connection)
        show_details["guests"] = _retrieve_not_my_job_info_by_id(show_id,
                                                                 database_connection)
        return show_details

    return None

def retrieve_all_details(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts with show details for all shows

    Arguments:
        database_connection (mysql.connector.connect)
    """
    show_ids = retrieve_all_ids(database_connection)
    if not show_ids:
        return None

    shows = []
    for show_id in show_ids:
        show_detail = retrieve_details_by_id(show_id,
                                             database_connection,
                                             pre_validated_id=True)

        if show_detail:
            shows.append(show_detail)

    return shows

def retrieve_details_by_date(show_year: int,
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
    show_id = convert_date_to_id(show_year,
                                 show_month,
                                 show_day,
                                 database_connection)
    if show_id:
        return retrieve_details_by_id(show_id, database_connection)

    return None

def retrieve_details_by_date_string(show_date: str,
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

    show_id = convert_date_to_id(parsed_show_date.year,
                                 parsed_show_date.month,
                                 parsed_show_date.day,
                                 database_connection)
    if show_id:
        return retrieve_details_by_id(show_id, database_connection)

    return None

def retrieve_details_by_year(show_year: int,
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
            show_detail = retrieve_details_by_id(show["showid"],
                                                 database_connection,
                                                 pre_validated_id=True)
            shows.append(show_detail)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_details_by_year_month(show_year: int,
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
            show_detail = retrieve_details_by_id(show["showid"],
                                                 database_connection,
                                                 pre_validated_id=True)
            shows.append(show_detail)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_recent_details(database_connection: mysql.connector.connect,
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
            show_detail = retrieve_details_by_id(show["showid"],
                                                 database_connection,
                                                 pre_validated_id=True)
            shows.append(show_detail)

        return shows
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
