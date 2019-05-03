# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Testing modules within the wwdtm package"""

import time
import json
import os
import mysql.connector
from tests import test_guest, test_host, test_panelist, test_scorekeeper, test_show

def test_guest_module(database_connection: mysql.connector.connect):
    """Run tests against guest module"""

    print("Testing wwdtm.guest module")

    # Start Time
    start_time = time.perf_counter()

    # Testing guest.id_exists
    test_guest.test_id_exists(database_connection)
    test_guest.test_id_not_exists(database_connection)

    # Testing guest.slug_exists
    test_guest.test_slug_exists(database_connection)
    test_guest.test_slug_not_exists(database_connection)

    # Testing retrieve all guests
    test_guest.test_retrieve_all(database_connection)
    test_guest.test_retrieve_all_ids(database_connection)

    # Testing retrieve individual guest
    test_guest.test_retrieve_by_id(database_connection)
    test_guest.test_retrieve_by_slug(database_connection)

    # Testing retrieve guest appearances
    test_guest.test_retrieve_appearances_by_id(database_connection)
    test_guest.test_retrieve_appearances_by_slug(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))
    return

def test_host_module(database_connection: mysql.connector.connect):
    """Run tests against host module"""

    print("Testing wwdtm.host module")

    # Start Time
    start_time = time.perf_counter()

    # Testing host.id_exists
    test_host.test_id_exists(database_connection)
    test_host.test_id_not_exists(database_connection)

    # Testing host.slug_exists
    test_host.test_slug_exists(database_connection)
    test_host.test_slug_not_exists(database_connection)

    # Testing retrieve all hosts
    test_host.test_retrieve_all(database_connection)
    test_host.test_retrieve_all_ids(database_connection)

    # Testing retrieve individual host
    test_host.test_retrieve_by_id(database_connection)
    test_host.test_retrieve_by_slug(database_connection)

    # Testing retrieve host appearances
    test_host.test_retrieve_appearances_by_id(database_connection)
    test_host.test_retrieve_appearances_by_slug(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))
    return


def test_panelist_module(database_connection: mysql.connector.connect):
    """Run tests against panelist module"""

    print("Testing wwdtm.panelist module")

    # Start Time
    start_time = time.perf_counter()

    # Testing panelist.id_exists
    test_panelist.test_id_exists(database_connection)
    test_panelist.test_id_not_exists(database_connection)

    # Testing panelist.slug_exists
    test_panelist.test_slug_exists(database_connection)
    test_panelist.test_slug_not_exists(database_connection)

    # Testing retrieve all panelists
    test_panelist.test_retrieve_all(database_connection)
    test_panelist.test_retrieve_all_ids(database_connection)

    # Testing retrieve individual panelist
    test_panelist.test_retrieve_by_id(database_connection)
    test_panelist.test_retrieve_by_slug(database_connection)

    # Testing retrieve panelist appearances
    test_panelist.test_retrieve_appearances_by_id(database_connection)
    test_panelist.test_retrieve_appearances_by_slug(database_connection)

    # Testing retrieve panelist statistics
    test_panelist.test_retrieve_statistics_by_id(database_connection)
    test_panelist.test_retrieve_statistics_by_invalid_id(database_connection)

    test_panelist.test_retrieve_statistics_by_slug(database_connection)
    test_panelist.test_retrieve_statistics_by_invalid_slug(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))
    return

def test_scorekeeper_module(database_connection: mysql.connector.connect):
    """Run tests against scorekeeper module"""

    print("Testing wwdtm.scorekeeper module")

    # Start Time
    start_time = time.perf_counter()

    # Testing scorekeeper.id_exists
    test_scorekeeper.test_id_exists(database_connection)
    test_scorekeeper.test_id_not_exists(database_connection)

    # Testing scorekeeper.slug_exists
    test_scorekeeper.test_slug_exists(database_connection)
    test_scorekeeper.test_slug_not_exists(database_connection)

    # Testing retrieve all scorekeepers
    test_scorekeeper.test_retrieve_all(database_connection)
    test_scorekeeper.test_retrieve_all_ids(database_connection)

    # Testing retrieve individual scorekeeper
    test_scorekeeper.test_retrieve_by_id(database_connection)
    test_scorekeeper.test_retrieve_by_slug(database_connection)

    # Testing retrieve scorekeeper appearances
    test_scorekeeper.test_retrieve_appearances_by_id(database_connection)
    test_scorekeeper.test_retrieve_appearances_by_slug(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))
    return

def test_show_module(database_connection: mysql.connector.connect):
    """Run tests against show module"""

    print("Testing wwdtm.show module")

    # Start Time
    start_time = time.perf_counter()

    # Testing show.id_exists
    test_show.test_id_exists(database_connection)
    test_show.test_id_not_exists(database_connection)

    # Testing show.date_exists
    test_show.test_date_exists(database_connection)
    test_show.test_date_not_exists(database_connection)

    # Testing retrieve show details
    test_show.test_retrieve_by_id(database_connection)
    test_show.test_retrieve_by_invalid_id(database_connection)

    test_show.test_retrieve_by_date(database_connection)
    test_show.test_retrieve_by_invalid_date(database_connection)

    test_show.test_retrieve_by_date_string(database_connection)
    test_show.test_retrieve_by_invalid_date_string(database_connection)

    # Testing retrieve multiple show details
    test_show.test_retrieve_by_year(database_connection)
    test_show.test_retrieve_by_year_month(database_connection)

    # Testing retrieve recent show details
    test_show.test_retrieve_recent(database_connection)

    # Calculate time elapsed
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time Elapsed: {}s\n".format(round(elapsed_time, 5)))
    return

def load_config(app_environment):
    """Load configuration file from config.json"""
    with open('config.json', 'r') as config_file:
        config_dict = json.load(config_file)

    if app_environment.startswith("develop"):
        if "development" in config_dict:
            config = config_dict["development"]
        else:
            raise Exception("Unable to locate 'development' section in config file!")
    elif app_environment.startswith("prod"):
        if "production" in config_dict:
            config = config_dict['production']
        else:
            raise Exception("Unable to locate 'production' section in config file!")
    else:
        if "local" in config_dict:
            config = config_dict["local"]
        else:
            raise Exception("Unable to locate 'local' section in config file!")

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
