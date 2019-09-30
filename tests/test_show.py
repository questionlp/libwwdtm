# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.show"""

import json
import mysql.connector
from wwdtm.show import details, info, utility

def test_id_exists(show_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing repsonse from utility.id_exists"""
    response = utility.id_exists(show_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(show_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing repsonse from utility.id_exists with an invalid ID"""
    response = utility.id_exists(show_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_date_exists(show_year: int,
                     show_month: int,
                     show_day: int,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from utility.date_exists"""
    response = utility.date_exists(show_year,
                                   show_month,
                                   show_day,
                                   database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_date_not_exists(show_year: int,
                         show_month: int,
                         show_day: int,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from utility.date_exists with an incorrect date"""
    response = utility.date_exists(show_year,
                                   show_month,
                                   show_day,
                                   database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_by_id(show_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_by_id"""
    show_info = info.retrieve_by_id(show_id, database_connection)
    assert show_info is not None
    assert "best_of" in show_info
    assert "repeat_show" in show_info
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_by_invalid_id(show_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from info.retrieve_by_id with an invalid ID"""
    show_info = info.retrieve_by_id(show_id, database_connection)
    assert show_info is None
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_by_date(show_year: int,
                          show_month: int,
                          show_day: int,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_by_date"""
    show_info = info.retrieve_by_date(show_year,
                                      show_month,
                                      show_day,
                                      database_connection)
    assert show_info is not None
    assert "best_of" in show_info
    assert "repeat_show" in show_info
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_by_invalid_date(show_year: int,
                                  show_month: int,
                                  show_day: int,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from info.retrieve_by_date with incorrect date"""
    try:
        show_info = info.retrieve_by_date(show_year,
                                          show_month,
                                          show_day,
                                          database_connection)
        assert show_info is None
        if print_response:
            print(json.dumps(show_info, indent=2))
    except ValueError:
        assert True

def test_retrieve_by_date_string(show_date: str,
                                 database_connection: mysql.connector.connect,
                                 print_response: bool = False):
    """Testing response from info.retrieve_by_date_string"""
    show_info = info.retrieve_by_date_string(show_date, database_connection)
    assert show_info is not None
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_by_invalid_date_string(show_date: str,
                                         database_connection: mysql.connector.connect,
                                         print_response: bool = False):
    """Testing response from info.retrieve_by_date_string with an
    incorrect date"""
    try:
        show_info = info.retrieve_by_date_string(show_date, database_connection)
        assert show_info is None
        if print_response:
            print(json.dumps(show_info, indent=2))
    except ValueError:
        assert True

def test_retrieve_months_by_year(show_year: int,
                                 database_connection: mysql.connector.connect,
                                 print_response: bool = False):
    """Testing response from info.retrieve_months_by_year"""
    show_months = info.retrieve_months_by_year(show_year,
                                               database_connection)
    assert show_months is not None
    if print_response:
        print(json.dumps(show_months, indent=2))

def test_retrieve_years(database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_years"""
    show_years = info.retrieve_years(database_connection)
    assert show_years is not None
    if print_response:
        print(json.dumps(show_years, indent=2))

def test_retrieve_by_year(show_year: int,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response form info.retrieve_by_year"""
    show_info = info.retrieve_by_year(show_year, database_connection)
    assert show_info is not None
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_by_year_month(show_year: int,
                                show_month: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response form info.retrieve_by_year_month"""
    show_info = info.retrieve_by_year_month(show_year,
                                            show_month,
                                            database_connection)
    assert show_info is not None
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from info.retrieve_all"""
    show_info = info.retrieve_all(database_connection)
    assert show_info is not None
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_recent(database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from info.retrieve_recent"""
    show_info = info.retrieve_recent(database_connection)
    assert show_info is not None
    if print_response:
        print(json.dumps(show_info, indent=2))

def test_retrieve_details_by_id(show_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from details.retrieve_by_id"""
    show_details = details.retrieve_by_id(show_id, database_connection)
    assert show_details is not None
    assert "host" in show_details
    assert "scorekeeper" in show_details
    assert "panelists" in show_details
    assert "bluff" in show_details
    assert "guests" in show_details
    assert "description" in show_details
    assert "notes" in show_details
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_invalid_id(show_id: int,
                                        database_connection: mysql.connector.connect,
                                        print_response: bool = False):
    """Testing response from details.retrieve_by_id with an
    invalid ID"""
    show_details = details.retrieve_by_id(show_id, database_connection)
    assert show_details is None
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_date(show_year: int,
                                  show_month: int,
                                  show_day: int,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from details.retrieve_by_date"""
    show_details = details.retrieve_by_date(show_year,
                                            show_month,
                                            show_day,
                                            database_connection)
    assert show_details is not None
    assert "host" in show_details
    assert "scorekeeper" in show_details
    assert "panelists" in show_details
    assert "bluff" in show_details
    assert "guests" in show_details
    assert "description" in show_details
    assert "notes" in show_details
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_invalid_date(show_year: int,
                                          show_month: int,
                                          show_day: int,
                                          database_connection: mysql.connector.connect,
                                          print_response: bool = False):
    """Testing repsonse from details.retrieve_by_date with an incorrect
    date"""
    show_details = details.retrieve_by_date(show_year,
                                            show_month,
                                            show_day,
                                            database_connection)
    assert show_details is None
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_date_string(show_date: str,
                                         database_connection: mysql.connector.connect,
                                         print_response: bool = False):
    """Testing repsonse from details.retrieve_by_date_string"""
    show_details = details.retrieve_by_date_string(show_date,
                                                   database_connection)
    assert show_details is not None
    assert "host" in show_details
    assert "scorekeeper" in show_details
    assert "panelists" in show_details
    assert "bluff" in show_details
    assert "guests" in show_details
    assert "description" in show_details
    assert "notes" in show_details
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_invalid_date_string(show_date: str,
                                                 database_connection: mysql.connector.connect,
                                                 print_response: bool = False):
    """Testing response from details.retrieve_by_date_string with
    invalud date string"""
    show_details = details.retrieve_by_date_string(show_date,
                                                   database_connection)
    assert show_details is None
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_year(show_year: int,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from details.retrieve_by_year"""
    show_details = details.retrieve_by_year(show_year,
                                            database_connection)
    assert show_details is not None
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_details_by_year_month(show_year: int,
                                        show_month: int,
                                        database_connection: mysql.connector.connect,
                                        print_response: bool = False):
    """Testing response from details.retrieve_by_year_month"""
    show_details = details.retrieve_by_year_month(show_year,
                                                  show_month,
                                                  database_connection)
    assert show_details is not None
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from details.retrieve_all"""
    show_details = details.retrieve_all(database_connection)
    assert show_details is not None
    if print_response:
        print(json.dumps(show_details, indent=2))

def test_retrieve_recent_details(database_connection: mysql.connector.connect,
                                 print_response: bool = False):
    """Testing response from details.retrieve_recent"""
    show_details = details.retrieve_recent(database_connection)
    assert show_details is not None
    if print_response:
        print(json.dumps(show_details, indent=2))
