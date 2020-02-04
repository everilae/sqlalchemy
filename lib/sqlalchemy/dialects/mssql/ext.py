# mssql/ext.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .apply import Apply
from ...util.langhelpers import public_factory  # noqa

apply = public_factory(Apply._create_apply, ".dialects.mssql.apply")
outerapply = public_factory(Apply._create_outerapply, ".dialects.mssql.outerapply")
