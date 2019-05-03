# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.panelist"""
import mysql.connector
import wwdtm.panelist as panelist
from wwdtm.responsecode import ResponseCode

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.id_exists"""
    response, response_code = panelist.id_exists(10, database_connection)
    assert (response, response_code) == (True, ResponseCode.SUCCESS)

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.id_exists"""
    response, response_code = panelist.id_exists(-1, database_connection)
    assert (response, response_code) == (False, ResponseCode.NOT_FOUND)

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.slug_exists"""
    response, response_code = panelist.slug_exists("tom-bodett", database_connection)
    assert (response, response_code) == (True, ResponseCode.SUCCESS)

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.slug_exists"""
    response, response_code = panelist.slug_exists("dom-bodet", database_connection)
    assert (response, response_code) == (False, ResponseCode.NOT_FOUND)

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_all"""
    panelists, response_code = panelist.retrieve_all(database_connection)
    assert panelists is not None
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_all_ids"""
    panelist_ids, response_code = panelist.retrieve_all_ids(database_connection)
    assert panelist_ids is not None
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_by_id"""
    panelist_dict, response_code = panelist.retrieve_by_id(10, database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_by_slug"""
    panelist_dict, response_code = panelist.retrieve_by_slug("tom-bodett", database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_appearances_by_id(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_apperances_by_id"""
    appearances, response_code = panelist.retrieve_appearances_by_id(10, database_connection)
    assert appearances is not None
    assert "shows" in appearances
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_appearances_by_slug(database_connection):
    """Testing response from panelist.retrieve_apperances_by_slug"""
    appearances, response_code = panelist.retrieve_appearances_by_slug("tom-bodett",
                                                                       database_connection)
    assert appearances is not None
    assert "shows" in appearances
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_statistics_by_id(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_statistics_by_id"""
    statistics, response_code = panelist.retrieve_statistics_by_id(10, database_connection)
    assert panelist is not None
    assert "scoring" in statistics
    assert "ranking" in statistics
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_statistics_by_invalid_id(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_statistics_by_id with an invalid ID"""
    statistics, response_code = panelist.retrieve_statistics_by_id(-1, database_connection)
    assert statistics is None
    assert response_code == ResponseCode.NOT_FOUND

def test_retrieve_statistics_by_slug(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_statistics_by_slug"""
    statistics, response_code = panelist.retrieve_statistics_by_slug("tom-bodett",
                                                                     database_connection)
    assert statistics is not None
    assert "scoring" in statistics
    assert "ranking" in statistics
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_statistics_by_invalid_slug(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_statistics_by_slug with an invalid slug"""
    statistics, response_code = panelist.retrieve_statistics_by_id("dom-bodet",
                                                                   database_connection)
    assert statistics is None
    assert response_code == ResponseCode.BAD_REQUEST
