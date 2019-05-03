# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""
import mysql.connector
import wwdtm.host as host
from wwdtm.responsecode import ResponseCode

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from host.id_exists"""
    response, response_code = host.id_exists(1, database_connection)
    assert (response, response_code) == (True, ResponseCode.SUCCESS)

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from host.id_exists"""
    response, response_code = host.id_exists(-1, database_connection)
    assert (response, response_code) == (False, ResponseCode.NOT_FOUND)

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from host.slug_exists"""
    response, response_code = host.slug_exists("peter-sagal", database_connection)
    assert (response, response_code) == (True, ResponseCode.SUCCESS)

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from host.slug_exists"""
    response, response_code = host.slug_exists("peter-segal", database_connection)
    assert (response, response_code) == (False, ResponseCode.NOT_FOUND)

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_all"""
    hosts, response_code = host.retrieve_all(database_connection)
    assert hosts is not None
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_all_hosts"""
    host_ids, response_code = host.retrieve_all_ids(database_connection)
    assert host_ids is not None
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_by_id"""
    host_dict, response_code = host.retrieve_by_id(1, database_connection)
    assert host_dict is not None
    assert "id" in host_dict
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_by_slug"""
    host_dict, response_code = host.retrieve_by_slug("peter-sagal", database_connection)
    assert host_dict is not None
    assert "id" in host_dict
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_appearances_by_id(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_apperances_by_id"""
    appearances, response_code = host.retrieve_appearances_by_id(1, database_connection)
    assert appearances is not None
    assert "shows" in appearances
    assert response_code == ResponseCode.SUCCESS

def test_retrieve_appearances_by_slug(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_apperances_by_slug"""
    appearances, response_code = host.retrieve_appearances_by_slug("luke-burbank",
                                                                   database_connection)
    assert appearances is not None
    assert "shows" in appearances
    assert response_code == ResponseCode.SUCCESS
