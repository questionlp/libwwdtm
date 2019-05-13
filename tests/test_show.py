# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.show"""

import mysql.connector
import wwdtm.show as show

def test_id_exists(database_connection: mysql.connector.connect):
    """Testing repsonse from show.id_exists"""
    response = show.id_exists(1091, database_connection)
    assert response

def test_id_not_exists(database_connection: mysql.connector.connect):
    """Testing repsonse from show.id_exists with invalid ID"""
    response = show.id_exists(-1, database_connection)
    assert not response

def test_date_exists(database_connection: mysql.connector.connect):
    """Testing response from show.date_exists"""
    response = show.date_exists(2006, 8, 19, database_connection)
    assert response

def test_date_not_exists(database_connection: mysql.connector.connect):
    """Testing response from show.date_exists with incorrect date"""
    response = show.date_exists(2006, 8, 18, database_connection)
    assert not response

def test_retrieve_show_details_by_id(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_show_details_by_id"""
    show_details = show.retrieve_show_details_by_id(1091, database_connection)
    assert show_details is not None
    assert "host" in show_details
    assert "scorekeeper" in show_details
    assert "panelists" in show_details
    assert "bluff" in show_details
    assert "guests" in show_details
    assert "description" in show_details
    assert "notes" in show_details

def test_retrieve_show_details_by_invalid_id(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_show_details_by_id with invalid ID"""
    show_details = show.retrieve_show_details_by_id(-1, database_connection)
    assert show_details is None

def test_retrieve_show_details_by_date(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_show_details_by_date"""
    show_details = show.retrieve_show_details_by_date(2006, 8, 19, database_connection)
    assert show_details is not None
    assert "host" in show_details
    assert "scorekeeper" in show_details
    assert "panelists" in show_details
    assert "bluff" in show_details
    assert "guests" in show_details
    assert "description" in show_details
    assert "notes" in show_details

def test_retrieve_show_details_by_invalid_date(database_connection: mysql.connector.connect):
    """Testing repsonse from show.retrieve_show_details_by_date with incorrect date"""
    show_details = show.retrieve_show_details_by_date(2006, 8, 18, database_connection)
    assert show_details is None

def test_retrieve_show_details_by_date_string(database_connection: mysql.connector.connect):
    """Testing repsonse from show.retrieve_show_details_by_date_string"""
    show_details = show.retrieve_show_details_by_date_string('2006-08-19', database_connection)
    assert show_details is not None
    assert "host" in show_details
    assert "scorekeeper" in show_details
    assert "panelists" in show_details
    assert "bluff" in show_details
    assert "guests" in show_details
    assert "description" in show_details
    assert "notes" in show_details

def test_retrieve_show_details_by_invalid_date_string(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_show_details_by_date_string with invalud date string"""
    show_details = show.retrieve_show_details_by_date_string('2006-08-32', database_connection)
    assert show_details is None

def test_retrieve_show_details_by_year(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_show_details_by_year"""
    show_details = show.retrieve_show_details_by_year(2018, database_connection)
    assert show_details is not None

def test_retrieve_show_details_by_year_month(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_show_details_by_year_month"""
    show_details = show.retrieve_show_details_by_year_month(2006, 8, database_connection)
    assert show_details is not None

def test_retrieve_recent_show_details(database_connection: mysql.connector.connect):
    """Testing response from show.retrieve_recent_show_details"""
    show_details = show.retrieve_recent_show_details(database_connection)
    assert show_details is not None
