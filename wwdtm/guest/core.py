# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides core functions for retrieving guest
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.guest import utility

#region Core Functions
def retrieve_appearances_by_id(guest_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested guest ID

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    if not pre_validated_id:
        valid_id = utility.validate_id(guest_id, database_connection)
        if not valid_id:
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT ( "
                 "SELECT COUNT(gm.showid) FROM ww_showguestmap gm "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE s.bestof = 0 AND s.repeatshowid IS NULL AND "
                 "gm.guestid = %s ) AS regular, ( "
                 "SELECT COUNT(gm.showid) FROM ww_showguestmap gm "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE gm.guestid = %s ) AS allshows;")
        cursor.execute(query, (guest_id, guest_id,))
        result = cursor.fetchone()

        appearance_info = OrderedDict()
        appearance_counts = OrderedDict()
        appearance_counts['regular_shows'] = result["regular"]
        appearance_counts['all_shows'] = result["allshows"]

        query = ("SELECT gm.showid, s.showdate, s.bestof, s.repeatshowid, "
                 "gm.guestscore, gm.exception FROM ww_showguestmap gm "
                 "JOIN ww_guests g ON g.guestid = gm.guestid "
                 "JOIN ww_shows s ON s.showid = gm.showid "
                 "WHERE gm.guestid = %s "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (guest_id,))
        result = cursor.fetchall()
        cursor.close()

        if result:
            appearances = []
            for appearance in result:
                info = OrderedDict()
                info['date'] = appearance["showdate"].isoformat()
                info['best_of'] = bool(appearance["bestof"])
                info['repeat_show'] = bool(appearance["repeatshowid"])
                info['score'] = appearance["guestscore"]
                info['score_exception'] = bool(appearance["exception"])
                appearances.append(info)

            appearance_info['count'] = appearance_counts
            appearance_info['shows'] = appearances
        else:
            appearance_info['count'] = 0
            appearance_info['shows'] = None

        return appearance_info
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_appearances_by_slug(guest_slug: str,
                                 database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested guest slug

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
    """
    guest_id = utility.convert_slug_to_id(guest_slug, database_connection)
    if guest_id:
        return retrieve_appearances_by_id(guest_id, database_connection, True)

    return None

#endregion
