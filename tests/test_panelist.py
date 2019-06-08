# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.panelist"""

import json
import mysql.connector
import wwdtm.panelist as panelist

def test_id_exists(panelist_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from panelist.id_exists"""
    response = panelist.id_exists(panelist_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(panelist_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from panelist.id_exists"""
    response = panelist.id_exists(panelist_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(panelist_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from panelist.slug_exists"""
    response = panelist.slug_exists(panelist_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(panelist_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from panelist.slug_exists"""
    response = panelist.slug_exists(panelist_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from panelist.retrieve_all"""
    panelists = panelist.retrieve_all(database_connection)
    assert panelists is not None
    if print_response:
        print(json.dumps(panelists, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from panelist.retrieve_all_ids"""
    panelist_ids = panelist.retrieve_all_ids(database_connection)
    assert panelist_ids is not None
    if print_response:
        print(json.dumps(panelist_ids, indent=2))

def test_retrieve_by_id(panelist_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from panelist.retrieve_by_id"""
    panelist_dict = panelist.retrieve_by_id(panelist_id, database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_by_slug(panelist_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from panelist.retrieve_by_slug"""
    panelist_dict = panelist.retrieve_by_slug(panelist_slug,
                                              database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_details_by_id(panelist_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from panelist.retrieve_details_by_id"""
    panelist_dict = panelist.retrieve_details_by_id(panelist_id,
                                                    database_connection)
    assert panelist_dict is not None
    assert "statistics" in panelist_dict
    assert "appearances" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_details_by_slug(panelist_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from panelist.retrieve_details_by_slug"""
    panelist_dict = panelist.retrieve_details_by_slug(panelist_slug,
                                                      database_connection)
    assert panelist_dict is not None
    assert "statistics" in panelist_dict
    assert "appearances" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from panelist.retrieve_all_details"""
    panelists_dict = panelist.retrieve_all_details(database_connection)
    assert panelists_dict is not None
    if print_response:
        print(json.dumps(panelists_dict, indent=2))

def test_retrieve_scores_list_by_id(panelist_id: int,
                                    database_connection: mysql.connector.connect,
                                    print_response: bool = False):
    """Testing response from panelist.retrieve_scores_list_by_id"""
    score_list = panelist.retrieve_scores_list_by_id(panelist_id,
                                                     database_connection)
    assert score_list is not None
    assert "shows" in score_list
    assert "scores" in score_list
    assert len(score_list["shows"]) == len(score_list["scores"])
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_list_by_slug(panelist_slug: str,
                                      database_connection: mysql.connector.connect,
                                      print_response: bool = False):
    """Testing response from panelist.retrieve_scores_list_by_slug"""
    score_list = panelist.retrieve_scores_list_by_slug(panelist_slug,
                                                       database_connection)
    assert score_list is not None
    assert "shows" in score_list
    assert "scores" in score_list
    assert len(score_list["shows"]) == len(score_list["scores"])
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_ordered_pair_by_id(panelist_id: int,
                                            database_connection: mysql.connector.connect,
                                            print_response: bool = False):
    """Testing response from panelist.retrieve_scores_ordered_pair_by_id"""
    score_list = panelist.retrieve_scores_ordered_pair_by_id(panelist_id,
                                                             database_connection)
    assert score_list is not None
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_ordered_pair_by_slug(panelist_slug: str,
                                              database_connection: mysql.connector.connect,
                                              print_response: bool = False):
    """Testing response from panelist.retrieve_scores_ordered_pair_by_slug"""
    score_list = panelist.retrieve_scores_ordered_pair_by_slug(panelist_slug,
                                                               database_connection)
    assert score_list is not None
    if print_response:
        print(json.dumps(score_list, indent=2))
