# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing module for wwdtm.panelist"""

import json
import mysql.connector
from wwdtm.panelist import details, info, utility

def test_id_exists(panelist_id: int,
                   database_connection: mysql.connector.connect,
                   print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(panelist_id, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_id_not_exists(panelist_id: int,
                       database_connection: mysql.connector.connect,
                       print_response: bool = False):
    """Testing response from utility.id_exists"""
    response = utility.id_exists(panelist_id, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_exists(panelist_slug: str,
                     database_connection: mysql.connector.connect,
                     print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(panelist_slug, database_connection)
    assert response
    if print_response:
        print(json.dumps(response, indent=2))

def test_slug_not_exists(panelist_slug: str,
                         database_connection: mysql.connector.connect,
                         print_response: bool = False):
    """Testing response from utility.slug_exists"""
    response = utility.slug_exists(panelist_slug, database_connection)
    assert not response
    if print_response:
        print(json.dumps(response, indent=2))

def test_retrieve_all(database_connection: mysql.connector.connect,
                      print_response: bool = False):
    """Testing response from info.retrieve_all"""
    panelists = info.retrieve_all(database_connection)
    assert panelists is not None
    if print_response:
        print(json.dumps(panelists, indent=2))

def test_retrieve_all_ids(database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_all_ids"""
    panelist_ids = info.retrieve_all_ids(database_connection)
    assert panelist_ids is not None
    if print_response:
        print(json.dumps(panelist_ids, indent=2))

def test_retrieve_by_id(panelist_id: int,
                        database_connection: mysql.connector.connect,
                        print_response: bool = False):
    """Testing response from info.retrieve_by_id"""
    panelist_dict = info.retrieve_by_id(panelist_id, database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_by_slug(panelist_slug: str,
                          database_connection: mysql.connector.connect,
                          print_response: bool = False):
    """Testing response from info.retrieve_by_slug"""
    panelist_dict = info.retrieve_by_slug(panelist_slug,
                                          database_connection)
    assert panelist_dict is not None
    assert "id" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_details_by_id(panelist_id: int,
                                database_connection: mysql.connector.connect,
                                print_response: bool = False):
    """Testing response from details.retrieve_by_id"""
    panelist_dict = details.retrieve_by_id(panelist_id,
                                           database_connection)
    assert panelist_dict is not None
    assert "statistics" in panelist_dict
    assert "bluffs" in panelist_dict
    assert "appearances" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_details_by_slug(panelist_slug: str,
                                  database_connection: mysql.connector.connect,
                                  print_response: bool = False):
    """Testing response from details.retrieve_by_slug"""
    panelist_dict = details.retrieve_by_slug(panelist_slug,
                                             database_connection)
    assert panelist_dict is not None
    assert "statistics" in panelist_dict
    assert "bluffs" in panelist_dict
    assert "appearances" in panelist_dict
    if print_response:
        print(json.dumps(panelist_dict, indent=2))

def test_retrieve_all_details(database_connection: mysql.connector.connect,
                              print_response: bool = False):
    """Testing response from details.retrieve_all"""
    panelists_dict = details.retrieve_all(database_connection)
    assert panelists_dict is not None
    if print_response:
        print(json.dumps(panelists_dict, indent=2))

def test_retrieve_scores_grouped_list_by_id(panelist_id: int,
                                            database_connection: mysql.connector.connect,
                                            print_response: bool = False):
    """Testing response from info.retrieve_scores_grouped_list_by_id"""
    score_list = info.retrieve_scores_grouped_list_by_id(panelist_id,
                                                         database_connection)
    assert score_list is not None
    assert "score" in score_list
    assert "count" in score_list
    assert len(score_list["score"]) == len(score_list["count"])
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_grouped_list_by_slug(panelist_slug: str,
                                              database_connection: mysql.connector.connect,
                                              print_response: bool = False):
    """Testing response from info.retrieve_scores_grouped_list_by_slug"""
    score_list = info.retrieve_scores_grouped_list_by_slug(panelist_slug,
                                                           database_connection)
    assert score_list is not None
    assert "score" in score_list
    assert "count" in score_list
    assert len(score_list["score"]) == len(score_list["count"])
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_grouped_ordered_pair_by_id(panelist_id: int,
                                                    database_connection: mysql.connector.connect,
                                                    print_response: bool = False):
    """Testing response from info.retrieve_scores_grouped_ordered_pair_by_id"""
    score_list = info.retrieve_scores_grouped_ordered_pair_by_id(panelist_id,
                                                                 database_connection)
    assert score_list is not None
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_grouped_ordered_pair_by_slug(panelist_slug: str,
                                                      database_connection: mysql.connector.connect,
                                                      print_response: bool = False):
    """Testing response from info.retrieve_scores_grouped_ordered_pair_by_slug"""
    score_list = info.retrieve_scores_grouped_ordered_pair_by_slug(panelist_slug,
                                                                   database_connection)
    assert score_list is not None
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_list_by_id(panelist_id: int,
                                    database_connection: mysql.connector.connect,
                                    print_response: bool = False):
    """Testing response from info.retrieve_scores_list_by_id"""
    score_list = info.retrieve_scores_list_by_id(panelist_id,
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
    """Testing response from info.retrieve_scores_list_by_slug"""
    score_list = info.retrieve_scores_list_by_slug(panelist_slug,
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
    """Testing response from info.retrieve_scores_ordered_pair_by_id"""
    score_list = info.retrieve_scores_ordered_pair_by_id(panelist_id,
                                                         database_connection)
    assert score_list is not None
    if print_response:
        print(json.dumps(score_list, indent=2))

def test_retrieve_scores_ordered_pair_by_slug(panelist_slug: str,
                                              database_connection: mysql.connector.connect,
                                              print_response: bool = False):
    """Testing response from info.retrieve_scores_ordered_pair_by_slug"""
    score_list = info.retrieve_scores_ordered_pair_by_slug(panelist_slug,
                                                           database_connection)
    assert score_list is not None
    if print_response:
        print(json.dumps(score_list, indent=2))
