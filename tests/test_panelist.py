# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.panelist"""

import mysql.connector
import wwdtm.panelist as panelist

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.id_exists"""
    response = panelist.id_exists(10, database_connection)
    assert response

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.id_exists"""
    response = panelist.id_exists(-1, database_connection)
    assert not response

def test_slug_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.slug_exists"""
    response = panelist.slug_exists("tom-bodett", database_connection)
    assert response

def test_slug_not_exists(database_connection: mysql.connector.connect):
    """Testing response from panelist.slug_exists"""
    response = panelist.slug_exists("dom-bodet", database_connection)
    assert not response

def test_retrieve_all(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_all"""
    panelists = panelist.retrieve_all(database_connection)
    assert panelists is not None

def test_retrieve_all_ids(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_all_ids"""
    panelist_ids = panelist.retrieve_all_ids(database_connection)
    assert panelist_ids is not None

def test_retrieve_by_id(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_by_id"""
    panelist_dict = panelist.retrieve_by_id(10, database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict

def test_retrieve_by_slug(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_by_slug"""
    panelist_dict = panelist.retrieve_by_slug("tom-bodett", database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict

def test_retrieve_details_by_id(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_details_by_id"""
    panelist_dict = panelist.retrieve_details_by_id(14, database_connection)
    assert panelist_dict is not None
    assert "statistics" in panelist_dict
    assert "appearances" in panelist_dict

def test_retrieve_details_by_slug(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_details_by_slug"""
    panelist_dict = panelist.retrieve_details_by_slug('maeve-higgins', database_connection)
    assert panelist_dict is not None
    assert "statistics" in panelist_dict
    assert "appearances" in panelist_dict

def test_retrieve_all_details(database_connection: mysql.connector.connect):
    """Testing response from panelist.retrieve_all_details"""
    panelists_dict = panelist.retrieve_all_details(database_connection)
    assert panelists_dict is not None
