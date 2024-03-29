# WWDTM

## Note

This version of the Wait Wait Stats Library has been superceded by version 2.
The repository for the new version of the Stats Page is available at
[wwdtm](https://github.com/questionlp/wwdtm).

No further development or bugfixes will be made to this project and the
repository has been marked as read-only.

## Overview

Data Access Library to provide show, host, scoreekeeper, panelist and guest
details from an instance of the
[Wait Wait... Don't Tell Me! Stats Page](http://wwdt.me) database.

## Requirements

- Python 3.6 or newer (Python 2.x is not supported)
- MySQL or MariaDB database containing data from the Wait Wait... Don't Tell
  Me! Stats Page database

### Notes

Even though the library is currently being developed and tested against Python
3.6, the code makes explicit use of `collections.OrderedDict()` to preserve key
insertion order into specific dictionaries. This was done as the code was
originally developed on systems with earlier versions of Python 3 that not
preserve key insertion order for `dict()`.

That behavior has since changed with Python 3.6 and key insertion order for
standard `dict()` is now part of the language's specifications moving forward.

That said, all development and testing has already been migrated to Python 3.6
and there is no guarantee that the library will be 100% functional in any older
versions.

## Installation

A packaged version of the library is available for download and install via
`pip` by adding <https://wheels.wwdt.me/> to your Python index list at install
time:

```bash
pip3 install --extra-index-url https://wheels.wwdt.me/ wwdtm
```

`pip` will also install packages that are required to use the library,
including:

- mysql-connector
- numpy
- python-dateutil
- python-slugify

## How to Use

```python
from wwdtm import guest, host, location, panelist, scorekeeper, show

guest.info.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

host.info.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

location.info.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

panelist.info.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

scorekeeper.info.retrieve_by_id(id: int, database_connect: mysql.connector.connect)

show.info.retrieve_by_id(id: int, database_connect: mysql.connector.connect)
```

## Running Tests

1. Set up a venv in the current directory by running: `python3 -m venv venv`
2. Create a copy of `config.dist.json` and name it `config.json`
3. Edit `config.json` and fill in the `local` section with the appropriate
   MySQL/MariaDB connection information
4. Activate the venv by running: `source ${venv}/bin/activate`
5. Install any required packages via `pip`: `pip3 install -r requirements.txt`
6. Run the test script: `python3 test.py`

## Packaging

```bash
python3 setup.py bdist_wheel
```

## Contributing

If you would like contribute to this project, please make sure to review both
the [Code of Conduct](CODE_OF_CONDUCT.md) and the
[Contributing](CONTRIBUTING.md) documents in this repository.

## License

This library is licensed under the terms of the
[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
