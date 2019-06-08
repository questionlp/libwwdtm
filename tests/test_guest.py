# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.guest"""

import json
import mysql.connector
import wwdtm.guest as guest

def test_id_exists(guest_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from guest.id_exists"""
    response = guest.id_exists(guest_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(guest_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from guest.id_exists"""
    response = guest.id_exists(guest_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(guest_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from guest.slug_exists"""
    response = guest.slug_exists(guest_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(guest_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from guest.slug_exists"""
    response = guest.slug_exists(guest_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from guest.retrieve_all"""
    guests = guest.retrieve_all(database_connection)
    assert guests is not None
    if print_response:
        print(json.dumps(guests, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from guest.retrieve_all_ids"""
    guest_ids = guest.retrieve_all_ids(database_connection)
    assert guest_ids is not None
    if print_response:
        print(json.dumps(guest_ids, indent=2))

def test_retrieve_by_id(guest_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from guest.retrieve_by_id"""
    guest_dict = guest.retrieve_by_id(guest_id, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_by_slug(guest_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from guest.retrieve_by_slug"""
    guest_dict = guest.retrieve_by_slug(guest_slug, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_details_by_id(guest_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from guest.retrieve_details_by_id"""
    guest_dict = guest.retrieve_details_by_id(guest_id, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    assert "appearances" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_details_by_slug(guest_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from guest.retrieve_details_by_slug"""
    guest_dict = guest.retrieve_details_by_slug(guest_slug, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict
    assert "appearances" in guest_dict
    if print_response:
        print(json.dumps(guest_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from guest_retrieve_all_details"""
    guests_dict = guest.retrieve_all_details(database_connection)
    assert guests_dict is not None
    if print_response:
        print(json.dumps(guests_dict, indent=2))
