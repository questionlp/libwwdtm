# WWDTM

## Overview

Data Access Library to provide show, host, scoreekeeper, panelist and guest
details from an instance of the Wait Wait Don't Tell Me! Stats Page database.

## Requirements

* Python 3.6 or newer (earlier versions of 3.x have not been tested and are not guaranteed to work)
* MySQL or MariaDB database containing data from the Wait Wait Don't Tell Me! Stats Page database

## Installing

* `git clone https://bitbucket.org/questionlp/libwwdtm/`
* `cd libwwdtm`
* `python setup.py install`
* `pip3 install https://to.be.determined/wwdtm-x.y.w.whl`

## How to Use

```python
from wwdtm import host, guest, panelist, scorekeeper, show

host.retrieve_by_id(id: str, database_connect: mysql.connector.connect)

guest.retrieve_by_id(id: str, database_connect: mysql.connector.connect)

panelist.retrieve_by_id(id: str, database_connect: mysql.connector.connect)

scorekeeper.retrieve_by_id(id: str, database_connect: mysql.connector.connect)

show.retrieve_by_id(id: str, database_connect: mysql.connector.connect)
```

## Running Tests

1. Set up a venv in the current directory
2. Create a copy of `config.dist.json` and name it `config.json`
3. Edit `config.json` and fill in the `local` section with the appropriate MySQL/MariaDB connection information
4. Activate the venv by running: `source ${venv}/bin/activate`
5. Install any required packages via `pip`: `pip install -r requirements.txt`
6. Run the test script: `python test.py`

## Packaging

```python3 setup.py bdist_wheel```

## License

This library is licensed under the terms of the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
