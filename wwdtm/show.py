# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_shows table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
import datetime
import sys
from typing import List, Dict, Tuple
import dateutil.parser as parser
import mysql.connector
from wwdtm.responsecode import ResponseCode

def validate_id(show_id: int,
                database_connection: mysql.connector.connect
               ) -> Tuple[bool, ResponseCode]:
    """Validate show ID against database.

    Arguments:
        show_id (int): Show ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns True on success; otherwise, it will return False. Also,
        returns a ResponseCode IntEnum
    """
    try:
        show_id = int(show_id)
    except ValueError:
        return False, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT showid from ww_shows where showid = %s;"
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, ResponseCode.SUCCESS

        return False, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def convert_date_to_id(show_year: int,
                       show_month: int,
                       show_day: int,
                       database_connection: mysql.connector.connect
                      ) -> Tuple[int, ResponseCode]:
    """Return show database ID from show year, month and day.

    Arguments:
        show_year (int): Show's four digit year
        show_month (int): Show's one or two digit month
        show_day (int): Show's day of month
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns an OrderedDict with show information. Also returns a
        ReponseCode IntEnum
    """
    show_date = None
    try:
        show_date = datetime.datetime(show_year, show_month, show_day)
    except ValueError:
        return None, ResponseCode.BAD_REQUEST

    try:
        show_date_str = show_date.strftime("%Y-%m-%d")
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid from ww_shows WHERE showdate = %s;")
        cursor.execute(query, (show_date_str,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result["showid"], ResponseCode.SUCCESS

        return None, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def convert_id_to_date(show_id: int,
                       database_connection: mysql.connector.connect
                      ) -> Tuple[datetime.datetime, ResponseCode]:
    """Return show date based on the show ID from the database.

    Arguments:
        show_id (int): Show ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (datetime.datetime, ResponseCode): Returns the corresponding show ID. Also returns a
        ResponseCode IntEnum
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = "SELECT showdate FROM ww_shows WHERE showid = %s;"
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return result["showdate"], ResponseCode.SUCCESS

        return None, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def id_exists(show_id: int,
              database_connection: mysql.connector.connect
             ) -> Tuple[bool, ResponseCode]:
    """Return whether or not a show ID exists in the database.

    Arguments:
        show_id (int): Show ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns True if show ID exists, False otherwise. Also returns
        a ResponseCode IntEnum
    """
    return validate_id(show_id, database_connection)

def date_exists(show_year: int,
                show_month: int,
                show_day: int,
                database_connection: mysql.connector.connect
               ) -> Tuple[bool, ResponseCode]:
    """Return whether or not a show ID exists in the database.

    Arguments:
        show_year (int): Show's four digit year
        show_month (int): Show's one or two digit month
        show_day (int): Show's day of month
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (bool, ResponseCode): Returns an OrderedDict with show information. Also returns a
        ReponseCode IntEnum
    """
    show_date = None
    try:
        show_date = datetime.datetime(show_year, show_month, show_day)
    except ValueError:
        return None, ResponseCode.BAD_REQUEST

    try:
        show_date_str = show_date.strftime("%Y-%m-%d")
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid from ww_shows WHERE showdate = %s;")
        cursor.execute(query, (show_date_str,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return True, ResponseCode.SUCCESS

        return False, ResponseCode.NOT_FOUND
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_by_id(show_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False
                  ) -> Tuple[Dict, ResponseCode]:
    """Return show information based on the show ID in the database.

    Arguments:
        show_id (int): Show ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the show ID has been validated or not
    Returns:
        (OrderedDict, ResponseCode): Returns an OrderedDict containing show information. Also
        returns a ResponseCode IntEnum
    """
    if not pre_validated_id:
        (valid_id, response_code) = validate_id(show_id, database_connection)
        if not valid_id:
            return None, response_code

    show_details = collections.OrderedDict()

    # Pull in the base show data, including date, host, scorekeeeper and notes
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT s.showid, s.showdate, s.bestof, s.repeatshowid, "
                 "h.hostid, h.host, h.hostslug, hm.guest as hostguest, sk.scorekeeperid, "
                 "sk.scorekeeper, sk.scorekeeperslug, skm.guest as scorekeeperguest, "
                 "skm.description, sd.showdescription, sn.shownotes "
                 "FROM ww_shows s "
                 "JOIN ww_showhostmap hm ON hm.showid = s.showid "
                 "JOIN ww_hosts h on h.hostid = hm.hostid "
                 "JOIN ww_showskmap skm ON skm.showid = s.showid "
                 "JOIN ww_scorekeepers sk on sk.scorekeeperid = skm.scorekeeperid "
                 "JOIN ww_showdescriptions sd on sd.showid = s.showid "
                 "JOIN ww_shownotes sn on sn.showid = s.showid "
                 "WHERE s.showid = %s;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()
        cursor.close()

        if not result:
            return None, ResponseCode.NOT_FOUND

        show_details["id"] = show_id
        show_details["date"] = result["showdate"].strftime("%Y-%m-%d")
        show_details["bestOf"] = bool(result["bestof"])
        if result["repeatshowid"]:
            show_details["isRepeat"] = True
            (original_show_date, _) = convert_id_to_date(result["repeatshowid"],
                                                         database_connection)
            show_details["originalShowDate"] = original_show_date.strftime("%Y-%m-%d")
        else:
            show_details["isRepeat"] = False

        show_details["description"] = str(result["showdescription"]).strip()
        show_details["notes"] = str(result["shownotes"]).strip()

        show_host = collections.OrderedDict()
        show_host["id"] = result["hostid"]
        show_host["name"] = result["host"]
        show_host["slug"] = result["hostslug"]
        show_host["guest"] = bool(result["hostguest"])

        show_scorekeeper = collections.OrderedDict()
        show_scorekeeper["id"] = result["scorekeeperid"]
        show_scorekeeper["name"] = result["scorekeeper"]
        show_scorekeeper["slug"] = result["scorekeeperslug"]
        show_scorekeeper["guest"] = bool(result["scorekeeperguest"])
        show_scorekeeper["description"] = result["description"]

        show_details["host"] = show_host
        show_details["scorekeeper"] = show_scorekeeper
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

    # Pull in and parse show location data
    try:
        location_info = collections.OrderedDict()
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT l.city, l.state, l.venue FROM "
                 "ww_showlocationmap lm "
                 "JOIN ww_shows s on s.showid = lm.showid "
                 "JOIN ww_locations l on l.locationid = lm.locationid "
                 "WHERE s.showid = %s;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchone()

        if not result:
            show_details["location"] = None
        else:
            location_info["city"] = result["city"]
            location_info["state"] = result["state"]
            location_info["venue"] = result["venue"]

        show_details["location"] = location_info
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

    # Pull in and parse panelist data
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT pm.panelistid, p.panelist, p.panelistslug, pm.panelistlrndstart, "
                 "pm.panelistlrndcorrect, pm.panelistscore, pm.showpnlrank "
                 "FROM ww_showpnlmap pm "
                 "JOIN ww_panelists p on p.panelistid = pm.panelistid "
                 "WHERE pm.showid = %s "
                 "ORDER by pm.panelistscore DESC, pm.showpnlmapid ASC;")
        cursor.execute(query, (show_id,))
        result = cursor.fetchall()
        cursor.close()

        if not result:
            show_details["panelists"] = None
        else:
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

            show_details["panelists"] = panelists
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

    # Pull in and parse Bluff the Listener data
    try:
        bluff_info = collections.OrderedDict()
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT blm.chosenbluffpnlid, p.panelist, p.panelistslug "
                 "FROM ww_showbluffmap blm "
                 "JOIN ww_shows s ON s.showid = blm.showid "
                 "JOIN ww_panelists p ON p.panelistid = blm.chosenbluffpnlid "
                 "WHERE s.showid = %s;")
        cursor.execute(query, (show_id,))
        chosen_result = cursor.fetchone()
        cursor.close()

        chosen_bluff_info = collections.OrderedDict()
        if chosen_result:
            chosen_bluff_info["id"] = chosen_result["chosenbluffpnlid"]
            chosen_bluff_info["name"] = chosen_result["panelist"]
            chosen_bluff_info["slug"] = chosen_result["panelistslug"]
        else:
            chosen_bluff_info = None

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT blm.correctbluffpnlid, p.panelist, p.panelistslug "
                 "FROM ww_showbluffmap blm "
                 "JOIN ww_shows s ON s.showid = blm.showid "
                 "JOIN ww_panelists p ON p.panelistid = blm.correctbluffpnlid "
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

        show_details["bluff"] = bluff_info
    except mysql.connector.Error:
        print(sys.exc_info())
        return None, ResponseCode.ERROR

    # Pull in and parse Not My Job guest data
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT gm.guestid, g.guest, g.guestslug, gm.guestscore, gm.exception "
                 "FROM ww_showguestmap gm "
                 "JOIN ww_guests g on g.guestid = gm.guestid "
                 "JOIN ww_shows s on s.showid = gm.showid "
                 "WHERE gm.showid = %s "
                 "ORDER by gm.showguestmapid ASC;")
        cursor.execute(query, (show_id,))

        result = cursor.fetchall()
        cursor.close()

        if not result:
            show_details["guests"] = None
        else:
            guests = []
            for guest in result:
                guest_info = collections.OrderedDict()
                guest_info["id"] = guest["guestid"]
                guest_info["name"] = guest["guest"]
                guest_info["slug"] = guest["guestslug"]
                guest_info["score"] = guest["guestscore"]
                guest_info["scoreException"] = bool(guest["exception"])
                guests.append(guest_info)

            show_details["guests"] = guests
    except mysql.connector.Error:
        print(sys.exc_info())
        return None, ResponseCode.ERROR

    return show_details, ResponseCode.SUCCESS

def retrieve_by_date(show_year: int,
                     show_month: int,
                     show_day: int,
                     database_connection: mysql.connector.connect
                    ) -> Tuple[Dict, ResponseCode]:
    """Return show information based on the show date in the database.

    Arguments:
        show_year (int): Show's four digit year
        show_month (int): Show's one or two digit month
        show_day (int): Show's day of month
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (OrderedDict, ResponseCode): Returns an OrderedDict containing show information. Also
        returns a ResponseCode IntEnum
    """
    (show_id, response_code) = convert_date_to_id(show_year,
                                                  show_month,
                                                  show_day,
                                                  database_connection)
    if not show_id:
        return None, response_code

    return retrieve_by_id(show_id, database_connection)

def retrieve_by_date_string(show_date: str,
                            database_connection: mysql.connector.connect
                           ) -> Tuple[Dict, ResponseCode]:
    """Return show information based on the show date string.

    Arguments:
        show_date (str): Show date in YYYY-MM-DD format
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (OrderedDict, ResponseCode): Returns an OrderedDict containing show information. Also
        returns a ResponseCode IntEnum
    """
    try:
        parsed_show_date = parser.parse(show_date)
    except ValueError:
        return None, ResponseCode.BAD_REQUEST

    (show_id, response_code) = convert_date_to_id(parsed_show_date.year,
                                                  parsed_show_date.month,
                                                  parsed_show_date.day,
                                                  database_connection)
    if not show_id:
        return None, response_code

    return retrieve_by_id(show_id, database_connection)

def retrieve_by_year(show_year: int,
                     database_connection: mysql.connector.connect
                    ) -> Tuple[List[Dict], ResponseCode]:
    """Return show information based on the show year provided

    Arguments:
        show_year (int): Four digit year
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (List[OrderedDict], ResponseCode): Returns a list containing OrderedDicts with show
        information. Also returns a ResponseCode IntEnum
    """
    try:
        parsed_show_year = parser.parse("{}".format(show_year))
    except ValueError:
        return None, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows WHERE YEAR(showdate) = %s "
                 "ORDER BY showdate ASC;")
        cursor.execute(query, (parsed_show_year.year,))
        result = cursor.fetchall()

        if not result:
            return None, ResponseCode.NOT_FOUND

        shows = []
        for show in result:
            (show_detail, _) = retrieve_by_id(show["showid"],
                                              database_connection,
                                              pre_validated_id=True)
            shows.append(show_detail)

        return shows, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_by_year_month(show_year: int,
                           show_month: int,
                           database_connection: mysql.connector.connect
                          ) -> Tuple[List[Dict], ResponseCode]:
    """Return show information based on the show year and month provided

    Arguments:
        show_year (int): Four digit year
        show_month (int): One or two digit month
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (List[OrderedDict], ResponseCode): Returns a list containing OrderedDicts with show
        information. Also returns a ResponseCode IntEnum
    """
    try:
        parsed_show_year_month = parser.parse("{}-{}".format(show_year, show_month))
    except ValueError:
        return None, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows WHERE YEAR(showdate) = %s "
                 "AND MONTH(showdate) = %s ORDER BY showdate ASC;")
        cursor.execute(query, (parsed_show_year_month.year, parsed_show_year_month.month,))
        result = cursor.fetchall()

        if not result:
            return None, ResponseCode.NOT_FOUND

        shows = []
        for show in result:
            (show_detail, _) = retrieve_by_id(show["showid"],
                                              database_connection,
                                              pre_validated_id=True)
            shows.append(show_detail)

        return shows, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR

def retrieve_recent(database_connection: mysql.connector.connect,
                    include_days_ahead: int = 7,
                    include_days_back: int = 32,
                   ) -> Tuple[List[Dict], ResponseCode]:
    """Return recent show information.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
        include_days_ahead (int): Number of days in the future to include
        include_days_back (int): Number of days in the past to include
    Returns:
        (List[OrderedDict], ResponseCode): Returns a list containing OrderedDicts with recent
        show information. Also returns a ResponseCode IntEnum
    """
    try:
        past_days = int(include_days_back)
        future_days = int(include_days_ahead)
    except ValueError:
        return None, ResponseCode.BAD_REQUEST

    try:
        past_date = datetime.datetime.now() - datetime.timedelta(days=past_days)
        future_date = datetime.datetime.now() + datetime.timedelta(days=future_days)
    except OverflowError:
        return None, ResponseCode.BAD_REQUEST

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT showid FROM ww_shows WHERE showdate >= %s AND "
                 "showdate <= %s ORDER BY showdate ASC;")
        cursor.execute(query,
                       (past_date.strftime("%Y-%m-%d"),
                        future_date.strftime("%Y-%m-%d")))
        result = cursor.fetchall()

        if not result:
            return None, ResponseCode.NOT_FOUND

        shows = []
        for show in result:
            (show_detail, _) = retrieve_by_id(show["showid"],
                                              database_connection,
                                              pre_validated_id=True)
            shows.append(show_detail)

        return shows, ResponseCode.SUCCESS
    except mysql.connector.Error:
        return None, ResponseCode.ERROR
