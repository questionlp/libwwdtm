# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""

import json
import mysql.connector
import wwdtm.scorekeeper as scorekeeper

def test_id_exists(scorekeeper_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from scorekeeper.id_exists"""
    response = scorekeeper.id_exists(scorekeeper_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(scorekeeper_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from scorekeeper.id_exists"""
    response = scorekeeper.id_exists(scorekeeper_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(scorekeeper_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from scorekeeper.slug_exists"""
    response = scorekeeper.slug_exists(scorekeeper_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(scorekeeper_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from scorekeeper.slug_exists"""
    response = scorekeeper.slug_exists(scorekeeper_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from scorekeeper.retrieve_all"""
    scorekeepers = scorekeeper.retrieve_all(database_connection)
    assert scorekeepers is not None
    if print_response:
        print(json.dumps(scorekeepers, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from scorekeeper.retrieve_all_ids"""
    scorekeeper_ids = scorekeeper.retrieve_all_ids(database_connection)
    assert scorekeeper_ids is not None
    if print_response:
        print(json.dumps(scorekeeper_ids, indent=2))

def test_retrieve_by_id(scorekeeper_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from scorekeeper.retrieve_by_id"""
    scorekeeper_dict = scorekeeper.retrieve_by_id(scorekeeper_id,
                                                  database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_by_slug(scorekeeper_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from scorekeeper.retrieve_by_slug"""
    scorekeeper_dict = scorekeeper.retrieve_by_slug(scorekeeper_slug,
                                                    database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_details_by_id(scorekeeper_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from scorekeeper.retrieve_details_by_id"""
    scorekeeper_dict = scorekeeper.retrieve_details_by_id(scorekeeper_id,
                                                          database_connection)
    assert scorekeeper_dict is not None
    assert "appearances" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_details_by_slug(scorekeeper_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from scorekeeper.retrieve_apperances_by_slug"""
    scorekeeper_dict = scorekeeper.retrieve_details_by_slug(scorekeeper_slug,
                                                            database_connection)
    assert scorekeeper_dict is not None
    assert "appearances" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from scorekeeper.retrieve_all_details"""
    scorekeepers_dict = scorekeeper.retrieve_all_details(database_connection)
    assert scorekeepers_dict is not None
    if print_response:
        print(json.dumps(scorekeepers_dict, indent=2))
