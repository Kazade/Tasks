# Copyright 2011 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

"""Code for parsing Index definitions."""

from u1db import (
    errors,
    )


class Getter(object):
    """Get values from a document based on a specification."""

    def get(self, raw_doc):
        """Get a value from the document.

        :param raw_doc: a python dictionary to get the value from.
        :return: A list of values that match the description.
        """
        raise NotImplementedError(self.get)


class StaticGetter(Getter):
    """A getter that returns a defined value (independent of the doc)."""

    def __init__(self, value):
        """Create a StaticGetter.

        :param value: the value to return when get is called.
        """
        if value is None:
            self.value = []
        elif isinstance(value, list):
            self.value = value
        else:
            self.value = [value]

    def get(self, raw_doc):
        return self.value


class ExtractField(Getter):
    """Extract a field from the document."""

    def __init__(self, field):
        """Create an ExtractField object.

        When a document is passed to get() this will return a value
        from the document based on the field specifier passed to
        the constructor.

        None will be returned if the field is nonexistant, or refers to an
        object, rather than a simple type or list of simple types.

        :param field: a specifier for the field to return.
            This is either a field name, or a dotted field name.
        """
        self.field = field

    def get(self, raw_doc):
        for subfield in self.field.split('.'):
            if isinstance(raw_doc, dict):
                raw_doc = raw_doc.get(subfield)
            else:
                return []
        if isinstance(raw_doc, dict):
            return []
        if raw_doc is None:
            result = []
        elif isinstance(raw_doc, list):
            # Strip anything in the list that isn't a simple type
            result = [val for val in raw_doc
                      if not isinstance(val, (dict, list))]
        else:
            result = [raw_doc]
        return result


class Transformation(Getter):
    """A transformation on a value from another Getter."""

    name = None
    """The name that the transform has in a query string."""

    def __init__(self, inner):
        """Create a transformation.

        :param inner: the Getter to transform the value for.
        """
        self.inner = inner

    def get(self, raw_doc):
        inner_values = self.inner.get(raw_doc)
        assert isinstance(inner_values, list),\
            'get() should always return a list'
        return self.transform(inner_values)

    def transform(self, values):
        """Transform the values.

        This should be implemented by subclasses to transform the
        value when get() is called.

        :param values: the values from the other Getter
        :return: the transformed values.
        """
        raise NotImplementedError(self.transform)


class Lower(Transformation):
    """Lowercase a string.

    This transformation will return None for non-string inputs. However,
    it will lowercase any strings in a list, dropping any elements
    that are not strings.
    """

    name = "lower"

    def _can_transform(self, val):
        return isinstance(val, basestring)

    def transform(self, values):
        if not values:
            return []
        return [val.lower() for val in values if self._can_transform(val)]


class Number(Transformation):
    """Convert an integer to a zero padded string.

    This transformation will return None for non-integer inputs. However, it
    will transform any integers in a list, dropping any elements that are not
    integers.
    """

    name = 'number'

    def __init__(self, inner, number):
        super(Number, self).__init__(inner)
        self.padding = "%%0%sd" % number

    def _can_transform(self, val):
        return isinstance(val, int) and not isinstance(val, bool)

    def transform(self, values):
        """Transform any integers in values into zero padded strings."""
        if not values:
            return []
        return [self.padding % (v,) for v in values if self._can_transform(v)]


class Bool(Transformation):
    """Convert bool to string."""

    name = "bool"

    def _can_transform(self, val):
        return isinstance(val, bool)

    def transform(self, values):
        """Transform any booleans in values into strings."""
        if not values:
            return []
        return [('1' if v else '0') for v in values if self._can_transform(v)]


class SplitWords(Transformation):
    """Split a string on whitespace.

    This Getter will return [] for non-string inputs. It will however
    split any strings in an input list, discarding any elements that
    are not strings.
    """

    name = "split_words"

    def _can_transform(self, val):
        return isinstance(val, basestring)

    def transform(self, values):
        if not values:
            return []
        result = []
        for value in values:
            if self._can_transform(value):
                # TODO: This is quadratic to search the list linearly while we
                #       are appending to it. Consider using a set() instead.
                for word in value.split():
                    if word not in result:
                        result.append(word)
        return result


class IsNull(Transformation):
    """Indicate whether the input is None.

    This Getter returns a bool indicating whether the input is nil.
    """

    name = "is_null"

    def transform(self, values):
        return [len(values) == 0]


class Parser(object):
    """Parse an index expression into a sequence of transformations."""

    _transformations = {}
    _delimiters = '()'

    def _take_word(self, partial):
        for idx, char in enumerate(partial):
            if char in self._delimiters:
                return partial[:idx], partial[idx:]
        return partial, ''

    def parse(self, field):
        inner = self._inner_parse(field)
        return inner

    def _inner_parse(self, field):
        word, field = self._take_word(field)
        if field.startswith("("):
            # We have an operation
            if not field.endswith(")"):
                raise errors.IndexDefinitionParseError(
                    "Invalid transformation function: %s" % field)
            op = self._transformations.get(word, None)
            if op is None:
                raise errors.IndexDefinitionParseError(
                    "Unknown operation: %s" % word)
            if ',' in field:
                # XXX: The arguments should probably be cast to whatever types
                # they represent, but short of evaling them, I don't see an
                # easy way to do that without adding a lot of complexity.
                # Since there is only one operation with an extra argument, I'm
                # punting on this until we grow some more.
                args = [a.strip() for a in field[1:-1].split(',')]
                extracted = args[0]
            else:
                args = []
                extracted = field[1:-1]
            inner = self._inner_parse(extracted)
            return op(inner, *args[1:])
        else:
            if len(field) != 0:
                raise errors.IndexDefinitionParseError(
                    "Unhandled characters: %s" % (field,))
            if len(word) == 0:
                raise errors.IndexDefinitionParseError(
                    "Missing field specifier")
            if word.endswith("."):
                raise errors.IndexDefinitionParseError(
                    "Invalid field specifier: %s" % word)
            return ExtractField(word)

    def parse_all(self, fields):
        return [self.parse(field) for field in fields]

    @classmethod
    def register_transormation(cls, transform):
        assert transform.name not in cls._transformations, (
                "Transform %s already registered for %s"
                % (transform.name, cls._transformations[transform.name]))
        cls._transformations[transform.name] = transform


Parser.register_transormation(SplitWords)
Parser.register_transormation(Lower)
Parser.register_transormation(Number)
Parser.register_transormation(Bool)
Parser.register_transormation(IsNull)
