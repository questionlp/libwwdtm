# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.guest"""

import json
import mysql.connector
from wwdtm.guest import info, details, utility

def test_id_exists(guest_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(guest_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(guest_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(guest_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(guest_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(guest_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(guest_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(guest_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from info.retrieve_all"""
    guests = info.retrieve_all(database_connection)
    assert guests is not None
    if print_response:
        print(json.dumps(guests, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_all_ids"""
    guest_ids = info.retrieve_all_ids(database_connection)
    assert guest_ids is not None
    if print_response:
        print(json.dumps(guest_ids, indent=2))

def test_retrieve_by_id(guest_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_by_id"""
    guest_dict = info.retrieve_by_id(guest_id, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_by_slug(guest_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_by_slug"""
    guest_dict = info.retrieve_by_slug(guest_slug, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_details_by_id(guest_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from details.retrieve_by_id"""
    guest_dict = details.retrieve_by_id(guest_id, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    assert "appearances" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_details_by_slug(guest_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from details.retrieve_by_slug"""
    guest_dict = details.retrieve_by_slug(guest_slug, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    assert "appearances" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from details.retrieve_all"""
    guests_dict = details.retrieve_all(database_connection)
    assert guests_dict is not None
    if print_response:
        print(json.dumps(guests_dict, indent=2))
