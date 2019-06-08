# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""

import json
import mysql.connector
import wwdtm.host as host

def test_id_exists(host_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from host.id_exists"""
    response = host.id_exists(host_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(host_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from host.id_exists"""
    response = host.id_exists(host_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(host_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from host.slug_exists"""
    response = host.slug_exists(host_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(host_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from host.slug_exists"""
    response = host.slug_exists(host_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from host.retrieve_all"""
    hosts = host.retrieve_all(database_connection)
    assert hosts is not None
    if print_response:
        print(json.dumps(hosts, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from host.retrieve_all_hosts"""
    host_ids = host.retrieve_all_ids(database_connection)
    assert host_ids is not None
    if print_response:
        print(json.dumps(host_ids, indent=2))

def test_retrieve_by_id(host_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from host.retrieve_by_id"""
    host_dict = host.retrieve_by_id(host_id, database_connection)
    assert host_dict is not None
    assert "id" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_by_slug(host_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from host.retrieve_by_slug"""
    host_dict = host.retrieve_by_slug(host_slug, database_connection)
    assert host_dict is not None
    assert "id" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_details_by_id(host_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from host.retrieve_details_by_id"""
    host_dict = host.retrieve_details_by_id(host_id, database_connection)
    assert host_dict is not None
    assert "appearances" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_details_by_slug(host_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from host.retrieve_details_by_slug"""
    host_dict = host.retrieve_details_by_slug(host_slug, database_connection)
    assert host_dict is not None
    assert "appearances" in host_dict
    if print_response:
        print(json.dumps(host_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from host.retrieve_all_detials"""
    hosts_dict = host.retrieve_all_details(database_connection)
    assert hosts_dict is not None
    if print_response:
        print(json.dumps(hosts_dict, indent=2))
