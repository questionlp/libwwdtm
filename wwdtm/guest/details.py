# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving guest details from
the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from typing import List, Dict
import mysql.connector
from wwdtm.guest import core, info, utility

#region Retrieval Functions
def retrieve_by_id(guest_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with guest details based on the
    requested guest ID

    Arguments:
        guest_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(guest_id, database_connection):
            return None

    guest = info.retrieve_by_id(guest_id,
                                database_connection,
                                pre_validated_id=True)
    guest["appearances"] = core.retrieve_appearances_by_id(guest_id,
                                                           database_connection,
                                                           pre_validated_id=True)
    return guest

def retrieve_by_slug(guest_slug: str,
                     database_connection: mysql.connector.connect
                    ) -> Dict:
    """Returns an OrderedDict with guest details based on the
    requested guest slug

    Arguments:
        guest_slug (str)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the guest ID has
        been validated
    """
    guest_id = utility.convert_slug_to_id(guest_slug, database_connection)
    if guest_id:
        return retrieve_by_id(guest_id,
                              database_connection,
                              pre_validated_id=True)

    return None

def retrieve_all(database_connection: mysql.connector.connection
                ) -> List[Dict]:
    """Returns a list of OrderedDict with guest details for all guests

    Arguments:
        database_connection (mysql.connector.connect)
    """
    guest_ids = info.retrieve_all_ids(database_connection)
    if not guest_ids:
        return None

    guests = []
    for guest_id in guest_ids:
        guest_details = retrieve_by_id(guest_id,
                                       database_connection,
                                       pre_validated_id=True)
        if guest_details:
            guests.append(guest_details)

    return guests

#endregion
