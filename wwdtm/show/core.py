# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides core functions for retrieving show information
from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from slugify import slugify
from wwdtm.show import utility

#region Core Retrieval Functions
def retrieve_core_info_by_id(show_id: int,
                             database_connection: mysql.connector.connect
                            ) -> Dict:
    """Returns an OrderedDict with core information for the requested
    show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
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

        repeat_show_id = result["repeatshowid"]

        if result["showdescription"]:
            show_description = str(result["showdescription"]).strip()
        else:
            show_description = None

        if result["shownotes"]:
            show_notes = str(result["shownotes"]).strip()
        else:
            show_notes = None

        location_info = OrderedDict()
        location_info["city"] = result["city"]
        location_info["state"] = result["state"]
        location_info["venue"] = result["venue"]

        host_info = OrderedDict()
        host_info["id"] = result["hostid"]
        host_info["name"] = result["host"]
        if result["hostslug"]:
            host_info["slug"] = result["hostslug"]
        else:
            host_info["slug"] = slugify(host_info["name"])

        host_info["guest"] = bool(result["hostguest"])

        if result["description"]:
            scorekeeper_description = result["description"]
        else:
            scorekeeper_description = None

        scorekeeper_info = OrderedDict()
        scorekeeper_info["id"] = result["scorekeeperid"]
        scorekeeper_info["name"] = result["scorekeeper"]
        if result["scorekeeperslug"]:
            scorekeeper_info["slug"] = result["scorekeeperslug"]
        else:
            scorekeeper_info["slug"] = slugify(scorekeeper_info["name"])

        scorekeeper_info["guest"] = bool(result["scorekeeperguest"])
        scorekeeper_info["description"] = scorekeeper_description

        show_info = OrderedDict()
        show_info["id"] = show_id
        show_info["date"] = result["showdate"].isoformat()
        show_info["best_of"] = bool(result["bestof"])
        show_info["repeat_show"] = bool(repeat_show_id)

        if repeat_show_id:
            original_date = utility.convert_id_to_date(repeat_show_id,
                                                       database_connection)
            show_info["original_show_id"] = repeat_show_id
            show_info["original_show_date"] = original_date.isoformat()

        show_info["description"] = show_description
        show_info["notes"] = show_notes
        show_info["location"] = location_info
        show_info["host"] = host_info
        show_info["scorekeeper"] = scorekeeper_info

        return show_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_panelist_info_by_id(show_id: int,
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
                 "pm.panelistlrndstart as start, "
                 "pm.panelistlrndcorrect as correct, "
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
            if panelist["showpnlrank"]:
                panelist_rank = panelist["showpnlrank"]
            else:
                panelist_rank = None

            info = OrderedDict()
            info["id"] = panelist["panelistid"]
            info["name"] = panelist["panelist"]
            if panelist["panelistslug"]:
                info["slug"] = panelist["panelistslug"]
            else:
                info["slug"] = slugify(info["name"])

            info["lightning_round_start"] = panelist["start"]
            info["lightning_round_correct"] = panelist["correct"]
            info["score"] = panelist["panelistscore"]
            info["rank"] = panelist_rank
            panelists.append(info)

        return panelists
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_bluff_info_by_id(show_id: int,
                              database_connection: mysql.connector.connect
                             ) -> Dict:
    """Returns an OrderedDicts containing panelist bluff information
    for the requested show ID

    Arguments:
        show_id (int)
        database_connection (mysql.connector.connect)
    """
    try:
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

        if chosen_result:
            chosen_bluff_info = OrderedDict()
            chosen_bluff_info["id"] = chosen_result["chosenbluffpnlid"]
            chosen_bluff_info["name"] = chosen_result["panelist"]
            if chosen_result["panelistslug"]:
                chosen_bluff_info["slug"] = chosen_result["panelistslug"]
            else:
                chosen_bluff_info["slug"] = slugify(chosen_bluff_info["name"])
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

        if correct_result:
            correct_bluff_info = OrderedDict()
            correct_bluff_info["id"] = correct_result["correctbluffpnlid"]
            correct_bluff_info["name"] = correct_result["panelist"]
            if correct_result["panelistslug"]:
                correct_bluff_info["slug"] = correct_result["panelistslug"]
            else:
                correct_bluff_info["slug"] = slugify(correct_bluff_info["name"])
        else:
            correct_bluff_info = None

        bluff_info = OrderedDict()
        bluff_info["chosen_panelist"] = chosen_bluff_info
        bluff_info["correct_panelist"] = correct_bluff_info

        return bluff_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_guest_info_by_id(show_id: int,
                              database_connection: mysql.connector.connect
                             ) -> List[Dict]:
    """Returns a list of OrderedDicts containing guest information for
    the requested show ID

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
            info = OrderedDict()
            info["id"] = guest["guestid"]
            info["name"] = guest["guest"]
            if guest["guestslug"]:
                info["slug"] = guest["guestslug"]
            else:
                info["slug"] = slugify(guest["guestslug"])

            info["score"] = guest["guestscore"]
            info["score_exception"] = bool(guest["exception"])
            guests.append(info)

        return guests
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
