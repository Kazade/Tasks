#!/usr/bin/env python
# Copyright 2012 Canonical Ltd.
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
#
"""This is a script to take a .sql file and generate a C array from it."""

import sys

_template = """\
/**
 * Copyright 2012 Canonical Ltd.
 *
 * This file is part of u1db.
 *
 * This file was auto-generated using sql_to_c.py %(args)s
 * Do not edit it directly.
 *
 * u1db is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation.
 *
 * u1db is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with u1db.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

static const char *tmp[] = {
%(lines)s
};
const char **%(variable)s = tmp;
const int %(variable)s_len = sizeof(tmp) / sizeof(char*);
"""

def main(args):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('sqlfile', default=None,
        help='The file to parse',
        type=argparse.FileType('r'))
    p.add_argument('c_variable', default=None,
        help='The name of the C variable to generate.')
    p.add_argument('cfile', default=None,
        help='The file to generate',
        type=argparse.FileType('wb'))
    opts = p.parse_args(args)
    source_content = opts.sqlfile.read()
    hunks = source_content.split(';')
    processed_hunks = []
    for hunk in hunks:
        hunk = hunk.strip()
        if not hunk:
            continue
        c_lines = []
        for line in hunk.split('\n'):
            if not line:
                continue
            c_line = '    "%s\\n"' % (line,)
            c_lines.append(c_line)
        processed_hunks.append('\n'.join(c_lines))
    processed = ',\n\n'.join(processed_hunks)
    
    opts.cfile.write(_template % dict(args=args, variable=opts.c_variable, lines=processed))


if __name__ == '__main__':
    main(sys.argv[1:])
