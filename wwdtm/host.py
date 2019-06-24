# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions that query the ww_hosts table in the
Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError

#region Internal Functions
def _retrieve_appearances_by_id(host_id: int,
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

        appearance_counts = OrderedDict(regularShows=result["regular"],
                                        allShows=result["allshows"])

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
                appearance_info = OrderedDict(date=appearance["showdate"].isoformat(),
                                              isBestOfShow=bool(appearance["bestof"]),
                                              isShowRepeat=bool(appearance["repeatshowid"]),
                                              guest=bool(appearance["guest"]))
                appearances.append(appearance_info)

            return OrderedDict(count=appearance_counts, shows=appearances)

        return OrderedDict(count=0, shows=None)
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def _retrieve_appearances_by_slug(host_slug: str,
                                  database_connection: mysql.connector.connect
                                 ) -> List[Dict]:
    """Returns a list of OrderedDicts containing appearence information
    for the requested host slug

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    host_id = convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return _retrieve_appearances_by_id(host_id, database_connection, True)

    return None

#endregion

#region Utility Functions
def convert_slug_to_id(host_slug: str,
                       database_connection: mysql.connector.connect) -> int:
    """Returns a host ID based on the requested host slug

    Arguments:
        host_slug (str)
        database_connect (mysql.connector.connect)
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
    """Returns true or false based on whether or not the requested host
    ID is valid

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
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
    """Returns true or false based on whether or not the requested host
    slug is valid

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
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
    """Returns true or falsed based on whether or not a host ID exists

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
    """
    return validate_id(host_id, database_connection)

def slug_exists(host_slug: str,
                database_connection: mysql.connector.connect) -> bool:
    """Returns true or falsed based on whether or not a host slug
    exists

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    return validate_slug(host_slug, database_connection)

#endregion

#region Retrieval Functions
def retrieve_all(database_connection: mysql.connector.connect) -> List[Dict]:
    """Returns a list of OrderedDicts containing host information for
    all hosts

    Arguments:
        database_connection (mysql.connector.connect)
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
            host = OrderedDict(id=row["hostid"],
                               name=row["host"],
                               slug=row["hostslug"],
                               gender=row["hostgender"])
            hosts.append(host)

        return hosts
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_all_ids(database_connection: mysql.connector.connect
                    ) -> List[int]:
    """Returns a list of all host IDs, sorted by host names

    Arguments:
        database_connection (mysql.connector.connect)
    """
    try:
        cursor = database_connection.cursor()
        query = ("SELECT hostid FROM ww_hosts WHERE hostslug != 'none' "
                 "ORDER BY host ASC;")
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
    """Returns an OrderedDict with host information based on the
    requested host ID

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the host ID has
        been validated
    """
    if not pre_validated_id:
        if not validate_id(host_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT host, hostslug, hostgender FROM ww_hosts "
                 "WHERE hostid = %s;")
        cursor.execute(query, (host_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return OrderedDict(id=host_id,
                               name=result["host"],
                               slug=result["hostslug"],
                               gender=result["hostgender"])

        return None
    except ProgrammingError as err:
        raise ProgrammingError("Unable to query the database") from err
    except DatabaseError as err:
        raise DatabaseError("Unexpected database error") from err

def retrieve_by_slug(host_slug: str,
                     database_connection: mysql.connector.connect) -> Dict:
    """Returns an OrderedDict with host information based on the
    requested host slug

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    host_id = convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_by_id(host_id, database_connection, True)

    return None

def retrieve_details_by_id(host_id: int,
                           database_connection: mysql.connector.connect,
                           pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with host details based on the requested
    host ID

    Arguments:
        host_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the host ID has
        been validated
    """
    if not pre_validated_id:
        if not validate_id(host_id, database_connection):
            return None

    host = retrieve_by_id(host_id, database_connection, pre_validated_id=True)
    host["appearances"] = _retrieve_appearances_by_id(host_id,
                                                      database_connection,
                                                      pre_validated_id=True)
    return host

def retrieve_details_by_slug(host_slug: str,
                             database_connection: mysql.connector.connect
                            ) -> Dict:
    """Returns an OrderedDict with host details based on the requested
    host slug

    Arguments:
        host_slug (str)
        database_connection (mysql.connector.connect)
    """
    host_id = convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_details_by_id(host_id,
                                      database_connection,
                                      pre_validated_id=True)

    return None

def retrieve_all_details(database_connection: mysql.connector.connect
                        ) -> List[Dict]:
    """Returns a list of OrderedDicts with host details for all hosts

    Arguments:
        database_connection (mysql.connector.connect)
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
