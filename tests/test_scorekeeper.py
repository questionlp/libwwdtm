# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.scorekeeper"""

import json
import mysql.connector
from wwdtm.scorekeeper import details, info, utility

def test_id_exists(scorekeeper_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(scorekeeper_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(scorekeeper_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(scorekeeper_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(scorekeeper_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(scorekeeper_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(scorekeeper_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(scorekeeper_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from info.retrieve_all"""
    scorekeepers = info.retrieve_all(database_connection)
    assert scorekeepers is not None
    if print_response:
        print(json.dumps(scorekeepers, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_all_ids"""
    scorekeeper_ids = info.retrieve_all_ids(database_connection)
    assert scorekeeper_ids is not None
    if print_response:
        print(json.dumps(scorekeeper_ids, indent=2))

def test_retrieve_by_id(scorekeeper_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_by_id"""
    scorekeeper_dict = info.retrieve_by_id(scorekeeper_id, database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_by_slug(scorekeeper_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_by_slug"""
    scorekeeper_dict = info.retrieve_by_slug(scorekeeper_slug,
                                             database_connection)
    assert scorekeeper_dict is not None
    assert "id" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_details_by_id(scorekeeper_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from details.retrieve_by_id"""
    scorekeeper_dict = details.retrieve_by_id(scorekeeper_id,
                                              database_connection)
    assert scorekeeper_dict is not None
    assert "appearances" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_details_by_slug(scorekeeper_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from details.retrieve_by_slug"""
    scorekeeper_dict = details.retrieve_by_slug(scorekeeper_slug,
                                                database_connection)
    assert scorekeeper_dict is not None
    assert "appearances" in scorekeeper_dict
    if print_response:
        print(json.dumps(scorekeeper_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from details.retrieve_all"""
    scorekeepers_dict = details.retrieve_all(database_connection)
    assert scorekeepers_dict is not None
    if print_response:
        print(json.dumps(scorekeepers_dict, indent=2))
