# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.host"""

import mysql.connector
import wwdtm.host as host

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from host.id_exists"""
    response = host.id_exists(1, database_connection)
    assert response

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from host.id_exists"""
    response = host.id_exists(-1, database_connection)
    assert not response

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from host.slug_exists"""
    response = host.slug_exists("peter-sagal", database_connection)
    assert response

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from host.slug_exists"""
    response = host.slug_exists("peter-segal", database_connection)
    assert not response

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_all"""
    hosts = host.retrieve_all(database_connection)
    assert hosts is not None

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_all_hosts"""
    host_ids = host.retrieve_all_ids(database_connection)
    assert host_ids is not None

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_by_id"""
    host_dict = host.retrieve_by_id(1, database_connection)
    assert host_dict is not None
    assert "id" in host_dict

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_by_slug"""
    host_dict = host.retrieve_by_slug("peter-sagal", database_connection)
    assert host_dict is not None
    assert "id" in host_dict

def test_retrieve_details_by_id(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_details_by_id"""
    host_dict = host.retrieve_details_by_id(1, database_connection)
    assert host_dict is not None
    assert "appearances" in host_dict

def test_retrieve_details_by_slug(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_details_by_slug"""
    host_dict = host.retrieve_details_by_slug('luke-burbank', database_connection)
    assert host_dict is not None
    assert "appearances" in host_dict

def test_retrieve_all_details(database_connection: mysql.connector.connect):
    """Testing response from host.retrieve_all_detials"""
    hosts_dict = host.retrieve_all_details(database_connection)
    assert hosts_dict is not None
