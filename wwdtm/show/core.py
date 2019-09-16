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

        location_info = OrderedDict(city=result["city"],
                                    state=result["state"],
                                    venue=result["venue"])

        host_info = OrderedDict(id=result["hostid"],
                                name=result["host"],
                                slug=result["hostslug"],
                                guest=bool(result["hostguest"]))

        if result["description"]:
            scorekeeper_description = result["description"]
        else:
            scorekeeper_description = None

        scorekeeper_info = OrderedDict(id=result["scorekeeperid"],
                                       name=result["scorekeeper"],
                                       slug=result["scorekeeperslug"],
                                       guest=bool(result["scorekeeperguest"]),
                                       description=scorekeeper_description)

        show_info = OrderedDict(id=show_id,
                                date=result["showdate"].isoformat(),
                                best_of=bool(result["bestof"]),
                                repeat_show=bool(repeat_show_id),
                                original_show_date=None,
                                description=show_description,
                                notes=show_notes,
                                location=location_info,
                                host=host_info,
                                scorekeeper=scorekeeper_info)

        if repeat_show_id:
            original_date = utility.convert_id_to_date(repeat_show_id,
                                                       database_connection)
            show_info["original_show_date"] = original_date.isoformat()
        else:
            del show_info["original_show_date"]

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

            panelist_info = OrderedDict(id=panelist["panelistid"],
                                        name=panelist["panelist"],
                                        slug=panelist["panelistslug"],
                                        lightning_round_start=panelist["start"],
                                        lightning_round_correct=panelist["correct"],
                                        score=panelist["panelistscore"],
                                        rank=panelist_rank)
            panelists.append(panelist_info)

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
            chosen_bluff_info = OrderedDict(id=chosen_result["chosenbluffpnlid"],
                                            name=chosen_result["panelist"],
                                            slug=chosen_result["panelistslug"])
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
            correct_bluff_info = OrderedDict(id=correct_result["correctbluffpnlid"],
                                             name=correct_result["panelist"],
                                             slug=correct_result["panelistslug"])
        else:
            correct_bluff_info = None

        bluff_info = OrderedDict(chosen_panelist=chosen_bluff_info,
                                 correct_panelist=correct_bluff_info)

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
            guest_info = OrderedDict(id=guest["guestid"],
                                     name=guest["guest"],
                                     slug=guest["guestslug"],
                                     score=guest["guestscore"],
                                     score_exception=bool(guest["exception"]))
            guests.append(guest_info)

        return guests
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

#endregion
