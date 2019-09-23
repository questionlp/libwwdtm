# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing modules within the wwdtm package"""

import time
import json
import os
import mysql.connector
from tests import (test_guest, test_host, test_location, test_panelist,
                   test_scorekeeper, test_show)

def test_guest_module(database_connection: mysql.connector.connect):
    """Run tests against guest module"""

    print("Testing wwdtm.guest module")

    # Start Time
    start_time = time.perf_counter()

    # Testing guest.id_exists
    test_guest.test_id_exists(54, database_connection)
    test_guest.test_id_not_exists(-54, database_connection)

    # Testing guest.slug_exists
    test_guest.test_slug_exists("tom-hanks", database_connection)
    test_guest.test_slug_not_exists("thom-hanks", database_connection)

    # Testing retrieve all guests
    test_guest.test_retrieve_all(database_connection)
    test_guest.test_retrieve_all_ids(database_connection)
    test_guest.test_retrieve_all_details(database_connection)

    # Testing retrieve individual guest
    test_guest.test_retrieve_by_id(2, database_connection)
    test_guest.test_retrieve_by_slug("stephen-breyer", database_connection)

    # Testing retrieve guest appearances
    test_guest.test_retrieve_details_by_id(36, database_connection)
    test_guest.test_retrieve_details_by_slug("tina-fey", database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

def test_host_module(database_connection: mysql.connector.connect):
    """Run tests against host module"""

    print("Testing wwdtm.host module")

    # Start Time
    start_time = time.perf_counter()

    # Testing host.id_exists
    test_host.test_id_exists(1, database_connection)
    test_host.test_id_not_exists(-1, database_connection)

    # Testing host.slug_exists
    test_host.test_slug_exists("luke-burbank", database_connection)
    test_host.test_slug_not_exists("buke-lurbank", database_connection)

    # Testing retrieve all hosts
    test_host.test_retrieve_all(database_connection)
    test_host.test_retrieve_all_ids(database_connection)
    test_host.test_retrieve_all_details(database_connection)

    # Testing retrieve individual host
    test_host.test_retrieve_by_id(3, database_connection)
    test_host.test_retrieve_by_slug("adam-felber", database_connection)

    # Testing retrieve host details
    test_host.test_retrieve_details_by_id(18, database_connection)
    test_host.test_retrieve_details_by_slug("faith-salie",
                                            database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

def test_location_module(database_connection: mysql.connector.connect):
    """Run tests against location module"""

    print("Testing wwdtm.location module")

    # Start Time
    start_time = time.perf_counter()

    # Testing location.id_exists
    test_location.test_id_exists(2, database_connection)
    test_location.test_id_not_exists(-2, database_connection)

    # Testing location.retrieve_all
    test_location.test_retrieve_all(database_connection)

    # Testing location.retrieve_all_ids
    test_location.test_retrieve_all_ids(database_connection)

    # Testing location.retrieve_by_id
    test_location.test_retrieve_by_id(32, database_connection)

    # Testing location.retrieve_recordings_by_id
    test_location.test_retrieve_recordings_by_id(32, database_connection)

    # Testing location.retrieve_all_recordings
    test_location.test_retrieve_all_recordings(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

def test_panelist_module(database_connection: mysql.connector.connect):
    """Run tests against panelist module"""

    print("Testing wwdtm.panelist module")

    # Start Time
    start_time = time.perf_counter()

    # Testing panelist.utility.id_exists
    test_panelist.test_id_exists(10, database_connection)
    test_panelist.test_id_not_exists(-10, database_connection)

    # Testing panelist.utility.slug_exists
    test_panelist.test_slug_exists("faith-salie", database_connection)
    test_panelist.test_slug_not_exists("fait-sale", database_connection)

    # Testing retrieve all panelists
    test_panelist.test_retrieve_all(database_connection)
    test_panelist.test_retrieve_all_ids(database_connection)
    test_panelist.test_retrieve_all_details(database_connection)

    # Testing retrieve individual panelist
    test_panelist.test_retrieve_by_id(14, database_connection)
    test_panelist.test_retrieve_by_slug("luke-burbank", database_connection)

    # Testing retrieve panelist details
    test_panelist.test_retrieve_details_by_id(2, database_connection)
    test_panelist.test_retrieve_details_by_slug("tom-bodett",
                                                database_connection)

    # Testing retrieve panelist scores
    test_panelist.test_retrieve_scores_list_by_id(30, database_connection)
    test_panelist.test_retrieve_scores_list_by_slug("faith-salie",
                                                    database_connection)
    test_panelist.test_retrieve_scores_ordered_pair_by_id(30,
                                                          database_connection)
    test_panelist.test_retrieve_scores_ordered_pair_by_slug("faith-salie",
                                                            database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

def test_scorekeeper_module(database_connection: mysql.connector.connect):
    """Run tests against scorekeeper module"""

    print("Testing wwdtm.scorekeeper module")

    # Start Time
    start_time = time.perf_counter()

    # Testing scorekeeper.id_exists
    test_scorekeeper.test_id_exists(1, database_connection)
    test_scorekeeper.test_id_not_exists(-1, database_connection)

    # Testing scorekeeper.slug_exists
    test_scorekeeper.test_slug_exists("carl-kasell", database_connection)
    test_scorekeeper.test_slug_not_exists("carl-kassel", database_connection)

    # Testing retrieve all scorekeepers
    test_scorekeeper.test_retrieve_all(database_connection)
    test_scorekeeper.test_retrieve_all_ids(database_connection)
    test_scorekeeper.test_retrieve_all_details(database_connection)

    # Testing retrieve individual scorekeeper
    test_scorekeeper.test_retrieve_by_id(11, database_connection)
    test_scorekeeper.test_retrieve_by_slug("bill-kurtis", database_connection)

    # Testing retrieve scorekeeper details
    test_scorekeeper.test_retrieve_details_by_id(2, database_connection)
    test_scorekeeper.test_retrieve_details_by_slug("korva-coleman",
                                                   database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

def test_show_module(database_connection: mysql.connector.connect):
    """Run tests against show module"""

    print("Testing wwdtm.show module")

    # Start Time
    start_time = time.perf_counter()

    # Testing show.utility.id_exists
    test_show.test_id_exists(1083, database_connection)
    test_show.test_id_not_exists(-1083, database_connection)

    # Testing show.utility.date_exists
    test_show.test_date_exists(2006, 8, 19, database_connection)
    test_show.test_date_not_exists(2006, 8, 20, database_connection)

    # Testing retrieve basic show info
    test_show.test_retrieve_by_id(47, database_connection)
    test_show.test_retrieve_by_invalid_id(-47, database_connection)

    test_show.test_retrieve_by_date(2018, 10, 27, database_connection)
    test_show.test_retrieve_by_invalid_date(2018, 10, 28, database_connection)

    test_show.test_retrieve_by_date_string("2007-03-24", database_connection)
    test_show.test_retrieve_by_invalid_date_string("2007-03-",
                                                   database_connection)

    # Testing retrieve multiple basic show info
    test_show.test_retrieve_by_year(2006, database_connection)
    test_show.test_retrieve_by_year_month(2006, 8, database_connection)
    test_show.test_retrieve_all(database_connection)

    # Testing retrieve recent basic show info
    test_show.test_retrieve_recent(database_connection)

    # Testing retrieve show months by year and show years
    test_show.test_retrieve_months_by_year(2006, database_connection)
    test_show.test_retrieve_years(database_connection)

    # Testing retrieve show details
    test_show.test_retrieve_details_by_id(1083, database_connection)
    test_show.test_retrieve_details_by_invalid_id(-1083, database_connection)

    test_show.test_retrieve_details_by_date(2018, 10, 27, database_connection)
    test_show.test_retrieve_details_by_invalid_date(2018,
                                                    10,
                                                    2,
                                                    database_connection)

    test_show.test_retrieve_details_by_date_string("2007-03-24",
                                                   database_connection)
    test_show.test_retrieve_details_by_invalid_date_string("2007-03-02",
                                                           database_connection)

    # Testing retrieve multiple show details
    test_show.test_retrieve_details_by_year(2006, database_connection)
    test_show.test_retrieve_details_by_year_month(2006, 12, database_connection)
    test_show.test_retrieve_all_details(database_connection)

    # Testing retrieve recent show details
    test_show.test_retrieve_recent_details(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

def load_config(app_environment):
    """Load configuration file from config.json"""
    with open('config.json', 'r') as config_file:
        config_dict = json.load(config_file)

    if app_environment.startswith("develop"):
        if "development" in config_dict:
            config = config_dict["development"]
        else:
            raise Exception("Missing 'development' section in config file")
    elif app_environment.startswith("prod"):
        if "production" in config_dict:
            config = config_dict['production']
        else:
            raise Exception("Missing 'production' section in config file")
    else:
        if "local" in config_dict:
            config = config_dict["local"]
        else:
            raise Exception("Missing 'local' section in config file")

    return config

def main():
    """Execute the test runs against the wwdtm modules"""

    # Start Time
    start_time = time.perf_counter()

    app_environment = os.getenv("APP_ENV", "local").strip().lower()
    print("Application Environment: {}".format(app_environment))
    print()
    config = load_config(app_environment)

    database_connection = mysql.connector.connect(**config["database"])

    test_guest_module(database_connection)
    test_host_module(database_connection)
    test_location_module(database_connection)
    test_panelist_module(database_connection)
    test_scorekeeper_module(database_connection)
    test_show_module(database_connection)

    database_connection.close()

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Testing completed")
    print("Total Time Elapsed: {}s\n".format(round(elapsed_time, 5)))

    return

# Only run if executed as a script and not imported
if __name__ == '__main__':
    main()
