# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""

import json
import mysql.connector
from wwdtm.host import info, details, utility

def test_id_exists(host_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from host.utility.id_exists"""
    response = utility.id_exists(host_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(host_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(host_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(host_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(host_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(host_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(host_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from info.retrieve_all"""
    hosts = info.retrieve_all(database_connection)
    assert hosts is not None
    if print_response:
        print(json.dumps(hosts, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_all_hosts"""
    host_ids = info.retrieve_all_ids(database_connection)
    assert host_ids is not None
    if print_response:
        print(json.dumps(host_ids, indent=2))

def test_retrieve_by_id(host_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_by_id"""
    host_dict = info.retrieve_by_id(host_id, database_connection)
    assert host_dict is not None
    assert "id" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_by_slug(host_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_by_slug"""
    host_dict = info.retrieve_by_slug(host_slug, database_connection)
    assert host_dict is not None
    assert "id" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_details_by_id(host_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from details.retrieve_by_id"""
    host_dict = details.retrieve_by_id(host_id, database_connection)
    assert host_dict is not None
    assert "appearances" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_details_by_slug(host_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from details.retrieve_by_slug"""
    host_dict = details.retrieve_by_slug(host_slug, database_connection)
    assert host_dict is not None
    assert "appearances" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from details.retrieve_all"""
    hosts_dict = details.retrieve_all(database_connection)
    assert hosts_dict is not None
    if print_response:
        print(json.dumps(hosts_dict, indent=2))
