# mssql/apply.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import itertools

from ...sql import coercions
from ...sql import roles
from ...sql.base import _from_objects
from ...sql.selectable import FromClause
from ...sql.selectable import FromGrouping
from ...sql.selectable import Select
from ...sql.visitors import InternalTraversal
from ... import util


class Apply(FromClause):
    """represents an ``APPLY`` construct between two :class:`.FromClause`
    elements.

    The public constructor function for :class:`.Apply` is the module-level
    :func:`~.ext.apply()` function.

    .. seealso::

        :func:`~.ext.apply`

    """

    __visit_name__ = "apply"

    _traverse_internals = [
        ("left", InternalTraversal.dp_clauseelement),
        ("right", InternalTraversal.dp_clauseelement),
        ("isouter", InternalTraversal.dp_boolean),
    ]

    #_is_join = True

    def __init__(self, left, right, isouter=False):
        """Construct a new :class:`.Apply`.

        The usual entrypoint here is the `:func:`~.ext.apply`
        function.

        """
        self.left = coercions.expect(roles.FromClauseRole, left)
        self.right = coercions.expect(
            roles.FromClauseRole, right  #, explicit_subquery=True
        ).self_group()

        self.isouter = isouter

    @classmethod
    def _create_outerapply(cls, left, right):
        """Return an ``OUTER APPLY`` clause element.

        The returned object is an instance of :class:`.Apply`.

        :param left: The left side of the apply.

        :param right: The right side of the apply.

        """
        return cls(left, right, isouter=True)

    @classmethod
    def _create_apply(cls, left, right, isouter=False):
        """Produce a :class:`.Apply` object, given two :class:`.FromClause`
        expressions.

        E.g.::

            a = apply(department_table,
                      func.get_reports(department_table.dept_mgr_id))
            stmt = select([department_table.dept_id,
                           column("emp_id"),
                           column("emp_salary")]).select_from(a)

        would emit SQL along the lines of::

            SELECT department.dept_id, emp_id, emp_salary FROM department
            CROSS APPLY get_reports(department.dept_mgr_id)

        :param left: The left side of the apply.

        :param right: The right side of the apply.

        :param isouter: if True, render an OUTER APPLY, instead of CROSS APPLY.

        .. seealso::

            :class:`.Apply` - the type of object produced

        """
        return cls(left, right, isouter)

    @property
    def description(self):
        return "Apply object on %s(%d) and %s(%d)" % (
            self.left.description,
            id(self.left),
            self.right.description,
            id(self.right),
        )

    def is_derived_from(self, fromclause):
        return (
            fromclause is self
            or self.left.is_derived_from(fromclause)
            or self.right.is_derived_from(fromclause)
        )

    def self_group(self, against=None):
        return FromGrouping(self)

    def _populate_column_collection(self):
        columns = [c for c in self.left.columns]
        columns += [c for c in self.right.columns]

        # This might be tricky in case of APPLY; nothing prevents the right
        # side from producing duplicate rows.
        #self.primary_key.extend(c for c in columns if c.primary_key)

        self._columns._populate_separate_keys(
            (col._key_label, col) for col in columns
        )
        self.foreign_keys.update(
            itertools.chain(*[col.foreign_keys for col in columns])
        )

    def _refresh_for_new_column(self, column):
        super(Apply, self)._refresh_for_new_column(column)
        self.left._refresh_for_new_column(column)
        self.right._refresh_for_new_column(column)

    def select(self, whereclause=None, **kwargs):
        r"""Create a :class:`.Select` from this :class:`.Apply`.

        The equivalent long-hand form, given a :class:`.Apply` object
        ``a``, is::

            from sqlalchemy import select
            a = select([a.left, a.right], **kw).\
                        where(whereclause).\
                        select_from(a)

        :param whereclause: the WHERE criterion that will be sent to
          the :func:`select()` function

        :param \**kwargs: all other kwargs are sent to the
          underlying :func:`select()` function.

        """
        collist = [self.left, self.right]

        return Select(collist, whereclause, from_obj=[self], **kwargs)

    # TODO: def alias along the lines of Join.alias()

    @property
    def bind(self):
        return self.left.bind or self.right.bind

    @property
    def _hide_froms(self):
        return itertools.chain(
            *[_from_objects(x.left, x.right) for x in self._cloned_set]
        )

    @property
    def _from_objects(self):
        return [self] + self.left._from_objects + self.right._from_objects
