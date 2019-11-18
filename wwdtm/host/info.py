# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving host information from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from collections import OrderedDict
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
from slugify import slugify
from wwdtm.host import utility

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
            host = OrderedDict()
            host["id"] = row["hostid"]
            host["name"] = row["host"]
            if row["hostslug"]:
                host["slug"] = row["hostslug"]
            else:
                host["slug"] = slugify(host["name"])

            host["gender"] = row["hostgender"]
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
        if not utility.validate_id(host_id, database_connection):
            return None

    try:
        cursor = database_connection.cursor(dictionary=True)
        query = ("SELECT host, hostslug, hostgender FROM ww_hosts "
                 "WHERE hostid = %s;")
        cursor.execute(query, (host_id,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            host_info = OrderedDict()
            host_info["id"] = host_id
            host_info["name"] = result["host"]
            if result["hostslug"]:
                host_info["slug"] = result["hostslug"]
            else:
                host_info["slug"] = slugify(host_info["name"])

            host_info["gender"] = result["hostgender"]
            return host_info

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
    host_id = utility.convert_slug_to_id(host_slug, database_connection)
    if host_id:
        return retrieve_by_id(host_id, database_connection, True)

    return None

#endregion
