# WWDTM

## Overview

Data Access Library to provide show, host, scoreekeeper, panelist and guest
details from an instance of the Wait Wait Don't Tell Me! Stats Page database.

## Requirements

* Python 3.6 or newer (earlier versions of 3.x have not been tested and are not guaranteed to work)
* MySQL or MariaDB database containing data from the Wait Wait Don't Tell Me! Stats Page database

## Installation

A packaged version of the library is available for download and install via `pip` by adding
https://wheels.wwdt.me/ to your Python index list at install time:

```bash
pip3 install --extra-index-url https://wheels.wwdt.me/ wwdtm
```

`pip` will also install packages that are required to use the library, including:

 * mysql-connector
 * numpy
 * python-dateutil
 * python-slugify

## How to Use

```python
from wwdtm import guest, host, location, panelist, scorekeeper, show

guest.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

host.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

location.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

panelist.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

scorekeeper.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

show.retrieve_by_id(id: int, database_connect: mysql.connector.connect)
```

## Running Tests

1. Set up a venv in the current directory by running: `python3 -m venv venv`
2. Create a copy of `config.dist.json` and name it `config.json`
3. Edit `config.json` and fill in the `local` section with the appropriate MySQL/MariaDB connection information
4. Activate the venv by running: `source ${venv}/bin/activate`
5. Install any required packages via `pip`: `pip3 install -r requirements.txt`
6. Run the test script: `python3 test.py`

## Packaging

```bash
python3 setup.py bdist_wheel
```

## License

This library is licensed under the terms of the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
