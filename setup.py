# -*- coding: utf-8 -*-
# Copyright (c) 2018-2020 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
"""Package setup file"""

import setuptools

setuptools.setup(name="wwdtm",
                 version="1.2.1.3",
                 description="Wait Wait... Don't Tell Me! Data Access Library",
                 long_description=("Provides show, host, scorekeeper, panelist and guest details "
                                   "from an instance of the Wait Wait... Don't Tell Me! Stats Page "
                                   "database"),
                 classifiers=[
                     "Development Status :: 4 - Beta",
                     "Intended Audience :: Developers",
                     "License :: OSI Approved :: Apache Software License 2.0",
                     "Programming Language :: Python :: 3.6",
                     "Topic :: Software Development :: Libraries",
                 ],
                 url="http://linhpham.org/",
                 author="Linh Pham",
                 author_email="dev@wwdt.me",
                 license="Apache License 2.0",
                 packages=setuptools.find_packages(exclude=["tests"]),
                 package_dir={"wwdtm": "wwdtm"},
                 project_urls={
                     "Source": "https://github.com/questionlp/libwwdtm/",
                 },
                 python_requires=">=3.6",
                 install_requires=[
                     "mysql-connector-python",
                     "numpy",
                     "python-dateutil",
                     "python-slugify",
                 ],
                 include_package_data=True
                 )
