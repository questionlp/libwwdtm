# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides core functions for retrieving host
information from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from wwdtm.host import utility

#region Internal Functions
def retrieve_appearances_by_id(host_id: int,
                               database_connection: mysql.connector.connect,
                               pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearance information
    for the requested Host ID

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the host ID has
        been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(host_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT ( "
                 "SELECT COUNT(hm.showid) FROM ww_showhostmap hm "
                 "JOIN ww_shows s ON s.showid = hm.showid "
                 "WHERE s.bestof = 0 AND s.repeatshowid IS NULL AND "
                 "hm.hostid = %s ) AS regular, ( "
                 "SELECT COUNT(hm.showid) FROM ww_showhostmap hm "
                 "JOIN ww_shows s ON s.showid = hm.showid "
                 "WHERE hm.hostid = %s ) AS allshows;")
        cursor.execute(query, (host_id, host_id,))
        result = cursor.fetchone()

        appearance_info = OrderedDict()
        appearance_counts = OrderedDict()
        appearance_counts['regular_shows'] = result["regular"]
        appearance_counts['all_shows'] = result["allshows"]

        query = ("SELECT hm.showid, s.showdate, s.bestof, s.repeatshowid, "
                 "hm.guest FROM ww_showhostmap hm "
                 "JOIN ww_hosts h ON h.hostid = hm.hostid "
                 "JOIN ww_shows s ON s.showid = hm.showid "
                 "WHERE hm.hostid = %s "
                 "ORDER BY s.showdate ASC;")
        cursor.execute(query, (host_id,))
        result = cursor.fetchall()
        cursor.close()

        if result:
            appearances = []
            for appearance in result:
                info = OrderedDict()
                info['date'] = appearance["showdate"].isoformat()
                info['best_of'] = bool(appearance["bestof"])
                info['repeat_show'] = bool(appearance["repeatshowid"])
                info['guest'] = bool(appearance["guest"])
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

def retrieve_appearances_by_slug(host_slug: str,
                                 database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearence information
    for the requested host slug

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    host_id = utility.convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_appearances_by_id(host_id, database_connection, True)

    return None

#endregion
