# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""

import mysql.connector
import wwdtm.scorekeeper as scorekeeper

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.id_exists"""
    response = scorekeeper.id_exists(2, database_connection)
    assert response

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.id_exists"""
    response = scorekeeper.id_exists(-1, database_connection)
    assert not response

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.slug_exists"""
    response = scorekeeper.slug_exists("corey-flintoff", database_connection)
    assert response

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.slug_exists"""
    response = scorekeeper.slug_exists("bill-curtis", database_connection)
    assert not response

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_all"""
    scorekeepers = scorekeeper.retrieve_all(database_connection)
    assert scorekeepers is not None

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_all_ids"""
    scorekeeper_ids  = scorekeeper.retrieve_all_ids(database_connection)
    assert scorekeeper_ids is not None

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_by_id"""
    scorekeeper_dict = scorekeeper.retrieve_by_id(2, database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_by_slug"""
    scorekeeper_dict = scorekeeper.retrieve_by_slug("carl-kasell", database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict

def test_retrieve_appearances_by_id(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_apperances_by_id"""
    appearances = scorekeeper.retrieve_appearances_by_id(2, database_connection)
    assert appearances is not None
    assert "shows" in appearances

def test_retrieve_appearances_by_slug(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_apperances_by_slug"""
    appearances = scorekeeper.retrieve_appearances_by_slug("korva-coleman", database_connection)
    assert appearances is not None
    assert "shows" in appearances
