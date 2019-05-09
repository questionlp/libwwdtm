# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.guest"""

import mysql.connector
import wwdtm.guest as guest

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from guest.id_exists"""
    response = guest.id_exists(25, database_connection)
    assert response

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from guest.id_exists"""
    response = guest.id_exists(-1, database_connection)
    assert not response

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from guest.slug_exists"""
    response = guest.slug_exists("tom-hanks", database_connection)
    assert response

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from guest.slug_exists"""
    response = guest.slug_exists("tom-flanks", database_connection)
    assert not response

def test_retrieve_all(database_connection):
    """Testing response from guest.retrieve_all"""
    guests = guest.retrieve_all(database_connection)
    assert guests is not None

def test_retrieve_all_ids(database_connection):
    """Testing response from guest.retrieve_all_ids"""
    guest_ids = guest.retrieve_all_ids(database_connection)
    assert guest_ids is not None

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from guest.retrieve_by_id"""
    guest_dict = guest.retrieve_by_id(25, database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from guest.retrieve_by_slug"""
    guest_dict = guest.retrieve_by_slug("tom-hanks", database_connection)
    assert guest_dict is not None
    assert "id" in guest_dict

def test_retrieve_appearances_by_id(database_connection: mysql.connector.connect):
    """Testing response from guest.retrieve_apperances_by_id"""
    appearances = guest.retrieve_appearances_by_id(25, database_connection)
    assert appearances is not None
    assert "shows" in appearances

def test_retrieve_appearances_by_slug(database_connection: mysql.connector.connect):
    """Testing response from guest.retrieve_apperances_by_slug"""
    appearances = guest.retrieve_appearances_by_slug("tom-hanks", database_connection)
    assert appearances is not None
    assert "shows" in appearances
