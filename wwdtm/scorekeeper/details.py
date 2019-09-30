# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""This module provides functions for retrieving scorekeeper details
from the Wait Wait... Don't Tell Me! Stats Page Database.
"""

from typing import List, Dict
import mysql.connector
from wwdtm.scorekeeper import core, info, utility

#region Retrieval Functions
def retrieve_by_id(scorekeeper_id: int,
                   database_connection: mysql.connector.connect,
                   pre_validated_id: bool = False) -> Dict:
    """Returns an OrderedDict with scorekeeper details for the
    requested scorekeeper ID

    Arguments:
        scorekeeper_id (int)
        database_connection (mysql.connector.connect)
        pre_validated_id (bool): Flag whether or not the scorekeeper ID
        has been validated
    """
    if not pre_validated_id:
        if not utility.validate_id(scorekeeper_id, database_connection):
            return None

    scorekeeper = info.retrieve_by_id(scorekeeper_id,
                                      database_connection,
                                      pre_validated_id=True)
    scorekeeper["appearances"] = core.retrieve_appearances_by_id(scorekeeper_id,
                                                                 database_connection,
                                                                 pre_validated_id=True)
    return scorekeeper

def retrieve_by_slug(scorekeeper_slug: str,
                     database_connection: mysql.connector.connect
                    ) -> Dict:
    """Returns an OrderedDict with scorekeeper details based on the
    scorekeeper slug

    Arguments:
        scorekeeper_slug (str)
        database_connection (mysql.connector.connect): Database connect
        object
    """
    scorekeeper_id = utility.convert_slug_to_id(scorekeeper_slug, database_connection)
    if scorekeeper_id:
        return retrieve_by_id(scorekeeper_id,
                              database_connection,
                              pre_validated_id=True)
    return None

def retrieve_all(database_connection: mysql.connector.connect
                ) -> List[Dict]:
    """Returns a list of OrderedDicts with scorekeeper details for all
    scorekeepers

    Arguments:
        database_connection (mysql.connector.connect)
    """
    scorekeeper_ids = info.retrieve_all_ids(database_connection)
    if not scorekeeper_ids:
        return None

    scorekeepers = []
    for scorekeeper_id in scorekeeper_ids:
        scorekeeper = retrieve_by_id(scorekeeper_id,
                                     database_connection,
                                     pre_validated_id=True)
        if scorekeeper:
            scorekeepers.append(scorekeeper)

    return scorekeepers

#endregion
