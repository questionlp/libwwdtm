# -*- coding: utf-8 -*-
# Copyright (c) 2018-2019 Linh Pham
# wwdtm is relased under the terms of the Apache License 2.0
""" This module contains common response enums """

from enum import IntEnum

class ResponseCode(IntEnum):
    """Response code enums"""

    SUCCESS = 200
    BAD_REQUEST = 400
    NOT_FOUND = 404
    ERROR = 503
