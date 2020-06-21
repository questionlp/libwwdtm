# -*- coding: utf-8 -*-
# Copyright (c) 2018-2020 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.location"""

import json
import mysql.connector
from wwdtm.location import details, info, utility

def test_id_exists(location_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(location_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(location_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(location_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(location_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(location_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(location_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(location_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from info.retrieve_all"""
    response = info.retrieve_all(database_connection)
    assert response is not None
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_all_ids"""
    response = info.retrieve_all_ids(database_connection)
    assert response is not None
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_by_id(location_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_by_id"""
    location_dict = info.retrieve_by_id(location_id, database_connection)
    assert location_dict is not None
    assert "city" in location_dict
    assert "state" in location_dict
    assert "venue" in location_dict
    assert "slug" in location_dict
    if print_response:
        print(json.dumps(location_dict, indent=2))

def test_retrieve_by_slug(location_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_by_slug"""
    location_dict = info.retrieve_by_slug(location_slug, database_connection)
    assert location_dict is not None
    assert "city" in location_dict
    assert "state" in location_dict
    assert "venue" in location_dict
    assert "slug" in location_dict
    if print_response:
        print(json.dumps(location_dict, indent=2))

def test_retrieve_recordings_by_id(location_id: int,
                                   database_connection: mysql.connector.connect,
                                   print_response: bool = False):
    """Testing response from details.retrieve_recordings_by_id"""
    location_dict = details.retrieve_recordings_by_id(location_id,
                                                      database_connection)
    assert location_dict is not None
    assert "recordings" in location_dict
    if print_response:
        print(json.dumps(location_dict, indent=2))

def test_retrieve_recordings_by_slug(location_slug: str,
                                     database_connection: mysql.connector.connect,
                                     print_response: bool = False):
    """Testing response from details.retrieve_recordings_by_slug"""
    location_dict = details.retrieve_recordings_by_slug(location_slug,
                                                        database_connection)
    assert location_dict is not None
    assert "recordings" in location_dict
    if print_response:
        print(json.dumps(location_dict, indent=2))

def test_retrieve_all_recordings(database_connection: mysql.connector.connect,
                                 print_response: bool = False):
    """Testing response from details.retrieve_all_recordings"""
    locations_dict = details.retrieve_all_recordings(database_connection)
    assert locations_dict is not None
    if print_response:
        print(json.dumps(locations_dict, indent=2))
