# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_hosts table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

import collections
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_appearances_by_id(host_id: int,
                                database_connection: mysql.connector.connect,
                                pre_validated_id: bool = False) -> List[Dict]:
    """Returns a list of OrderedDicts containing information about all of the host's
    appearances.

    Arguments:
        host_id (int): Host ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the host ID has been validated
    Returns:
        list[OrderedDict]: Returns a list containing an OrderedDict with host
        appearance information
    """
    if not pre_validated_id:
        if not validate_id(host_id, database_connection):
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
        cursor.close()

        appearance_counts = collections.OrderedDict()
        appearance_counts["regularShows"] = result["regular"]
        appearance_counts["allShows"] = result["allshows"]

        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT hm.showid, s.showdate, s.bestof, s.repeatshowid, hm.guest "
                 "FROM ww_showhostmap hm "
                 "JOIN ww_hosts h ON h.hostid = hm.hostid "
                 "JOIN ww_shows s ON s.showid = hm.showid "
                 "WHERE hm.hostid = %s "
                 "ORDER BY s.showdate ASC;")

        cursor.execute(query, (host_id,))
        result = cursor.fetchall()
        cursor.close()

        appearance_dict = collections.OrderedDict()
        if result:
            appearances = []
            for appearance in result:
                appearance_info = {}
                appearance_info["date"] = appearance["showdate"].isoformat()
                appearance_info["isBestOfShow"] = bool(appearance["bestof"])
                appearance_info["isShowRepeat"] = bool(appearance["repeatshowid"])
                appearance_info["guest"] = bool(appearance["guest"])
                appearances.append(appearance_info)

            appearance_dict["count"] = appearance_counts
            appearance_dict["shows"] = appearances
        else:
            appearance_dict["count"] = 0
            appearance_dict["shows"] = None

        return appearance_dict
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_appearances_by_slug(host_slug: str,
                                  database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts containing information about all of the host's
    appearances.

    Arguments:
        host_slug (str): Host slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (list[OrderedDict], ResponseCode): Returns a list containing an OrderedDict with host
        appearance information. Also returns a ReponseCode IntEnum
    """
    host_id = convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return _retrieve_appearances_by_id(host_id, database_connection, True)

    return None

#endregion

#region Utility Functions
def convert_slug_to_id(host_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Return host database ID from slug string.

    Arguments:
        host_slug (str): Host slug string
        database_connect (mysql.connector.connect): Database connect object

    Returns:
        int: Returns host ID on success; otherwise returns None
    """
    try:
        cursor = database_connection.cursor()
        query = "SELECT hostid FROM ww_hosts WHERE hostslug = %s;"
        cursor.execute(query, (host_slug,))

        result = cursor.fetchone()
        cursor.close()

        if result:
            return result[0]

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_id(host_id: int,
                database_connection: mysql.connector.connect) -> bool:
    """Validate host ID against database

    Arguments:
        host_id (int); Host ID from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        bool: Returns True on success, otherwise returns False
    """
    try:
        host_id = int(host_id)
    except ValueError:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT hostid FROM ww_hosts WHERE hostid = %s;"
        cursor.execute(query, (host_id,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def validate_slug(host_slug: str,
                  database_connection: mysql.connector.connect) -> bool:
    """Validate host slug string against database

    Arguments:
        host_slug (str): Host slug string from database
        database_connection (mysql.connector.connect): Database connect object

    Returns:
        bool: Returns True if host slug is valid, otherwise returns False
    """
    host_slug = host_slug.strip()
    if not host_slug:
        return False

    try:
        cursor = database_connection.cursor()
        query = "SELECT hostslug FROM ww_hosts WHERE hostslug = %s;"
        cursor.execute(query, (host_slug,))

        result = cursor.fetchone()
        cursor.close()

        return bool(result)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def id_exists(host_id: int,
              database_connection: mysql.connector.connect) -> bool:
    """Return whether or not a host ID exists in the database.

    Arguments:
        host_id (int): Host ID from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        bool: Returns True if host ID exists, otherwise returns False
    """
    return validate_id(host_id, database_connection)

def slug_exists(host_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Return whether or not a host slug exists in the database.

    Arguments:
        host_slug (int): Host slug from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        bool: Returns True if host slug exists, otherwise returns False
    """
    return validate_slug(host_slug, database_connection)

#endregion

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Return a list of OrderedDicts containing hosts and their details.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[OrderedDict]: Returns a list containing an OrderedDict of host details
    """
    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT hostid, host, hostslug, hostgender FROM ww_hosts "
                 "WHERE hostslug != 'tbd' ORDER BY host ASC;")
        cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()

        hosts = []
        for row in result:
            host = collections.OrderedDict()
            host["id"] = row["hostid"]
            host["name"] = row["host"]
            host["slug"] = row["hostslug"]
            host["gender"] = row["hostgender"]
            hosts.append(host)

        return hosts
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect) -> List[int]:
    """Return a list of all host IDs, with IDs sorted in the order of host names.

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        list[int]: Returns a list containing host IDs
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT hostid FROM ww_hosts WHERE hostslug != 'none' ORDER BY host ASC;")
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

def retrieve_by_id(host_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with host information based on the host ID.

    Arguments:
        host_id (int): Host ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the host ID has been validated
    Returns:
        OrderedDict: Returns an OrderedDict containing host id, name, and slug string
    """
    if not pre_validated_id:
        if not validate_id(host_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT host, hostslug, hostgender FROM ww_hosts WHERE hostid = %s;")

        cursor.execute(query, (host_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            host_dict = collections.OrderedDict()
            host_dict = {
                "id": host_id,
                "name": result["host"],
                "slug": result["hostslug"],
                "gender": result["hostgender"]
                }
            return host_dict

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_slug(host_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with host details based on the host slug string

    Arguments:
        host_slug (str): Host slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (OrderedDict, ResponseCode): Returns an OrderedDict containing host id, name and slug
        string
    """
    host_id = convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_by_id(host_id, database_connection, True)

    return None

def retrieve_details_by_id(host_id: int,
                           database_connection: mysql.connector.connect,
                           pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with host information and appearances based on the host ID.

    Arguments:
        host_id (int): Host ID from database
        database_connection (mysql.connector.connect): Database connect object
        pre_validated_id (bool): Flag whether or not the host ID has been validated
    Returns:
        OrderedDict: Returns an OrderedDict containing host id, name, and slug string
    """
    if not pre_validated_id:
        if not validate_id(host_id, database_connection):
            return None

    host = retrieve_by_id(host_id, database_connection, pre_validated_id=True)
    host["appearances"] = _retrieve_appearances_by_id(host_id,
                                                      database_connection,
                                                      pre_validated_id=True)
    return host

def retrieve_details_by_slug(host_slug: str, database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with host information and appearances based on the host slug string

    Arguments:
        host_slug (str): Host slug string from database
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        (OrderedDict, ResponseCode): Returns an OrderedDict containing host id, name and slug
        string
    """
    host_id = convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_details_by_id(host_id,
                                      database_connection,
                                      pre_validated_id=True)

    return None

def retrieve_all_details(database_connection: mysql.connector.connect) -> List[Dict]:
    """Return detailed information for all hosts in the database

    Arguments:
        database_connection (mysql.connector.connect): Database connect object
    Returns:
        List[OrderedDict]: Returns a list of OrderedDicts containing host details
    """
    host_ids = retrieve_all_ids(database_connection)
    if not host_ids:
        return None

    hosts = []
    for host_id in host_ids:
        host = retrieve_details_by_id(host_id,
                                      database_connection,
                                      pre_validated_id=True)
        if host:
            hosts.append(host)

    return hosts

#endregion
