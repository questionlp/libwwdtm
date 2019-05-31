# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.location"""

import mysql.connector
import wwdtm.location as location

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from location.id_exists"""
    response = location.id_exists(2, database_connection)
    assert response

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from location.id_exists"""
    response = location.id_exists(-2, database_connection)
    assert not response

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from location.retrieve_all"""
    response = location.retrieve_all(database_connection)
    assert response is not None

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from location.retrieve_all_ids"""
    response = location.retrieve_all_ids(database_connection)
    assert response is not None

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from location.retrieve_by_id"""
    location_dict = location.retrieve_by_id(2, database_connection)
    print(location_dict)
    assert location_dict is not None
    assert "city" in location_dict
    assert "state" in location_dict
    assert "venue" in location_dict

def test_retrieve_recordings_by_id(database_connection: mysql.connector.connect):
    """Testing response from location.retrieve_recordings_by_id"""
    location_dict = location.retrieve_recordings_by_id(32, database_connection)
    print(location_dict)
    assert location_dict is not None
    assert "recordings" in location_dict

def test_retrieve_all_recordings(database_connection: mysql.connector.connect):
    """Testing response from location.retrieve_all_recordings"""
    locations_dict = location.retrieve_all_recordings(database_connection)
    assert locations_dict is not None
