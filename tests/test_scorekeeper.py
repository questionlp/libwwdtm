# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""
import mysql.connector
import wwdtm.scorekeeper as scorekeeper
from wwdtm.responsecode import ResponseCode

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.id_exists"""
    response, response_code = scorekeeper.id_exists(2, database_connection)
    assert (response, response_code) == (True, ResponseCode.SUCCESS)

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.id_exists"""
    response, response_code = scorekeeper.id_exists(-1, database_connection)
    assert (response, response_code) == (False, ResponseCode.NOT_FOUND)

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.slug_exists"""
    response, response_code = scorekeeper.slug_exists("corey-flintoff", database_connection)
    assert (response, response_code) == (True, ResponseCode.SUCCESS)

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.slug_exists"""
    response, response_code = scorekeeper.slug_exists("bill-curtis", database_connection)
    assert (response, response_code) == (False, ResponseCode.NOT_FOUND)

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_all"""
    scorekeepers, response_code = scorekeeper.retrieve_all(database_connection)
    assert scorekeepers is not None
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_all_ids"""
    scorekeeper_ids, response_code = scorekeeper.retrieve_all_ids(database_connection)
    assert scorekeeper_ids is not None
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_by_id"""
    scorekeeper_dict, response_code = scorekeeper.retrieve_by_id(2, database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_by_slug"""
    scorekeeper_dict, response_code = scorekeeper.retrieve_by_slug("carl-kasell",
                                                                   database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_appearances_by_id(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_apperances_by_id"""
    appearances, response_code = scorekeeper.retrieve_appearances_by_id(2, database_connection)
    assert appearances is not None
    assert "shows" in appearances
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_appearances_by_slug(database_connection: mysql.connector.connect):
    """Testing response from scorekeeper.retrieve_apperances_by_slug"""
    appearances, response_code = scorekeeper.retrieve_appearances_by_slug("korva-coleman",
                                                                          database_connection)
    assert appearances is not None
    assert "shows" in appearances
    assert response_code == ResponseCode.SUCCESS
