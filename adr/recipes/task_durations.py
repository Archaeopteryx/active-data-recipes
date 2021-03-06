from __future__ import print_function, absolute_import

import json
from collections import defaultdict

from ..cli import RecipeParser
from ..query import format_date, run_query


def run(args):
    parser = RecipeParser('date')
    parser.add_argument('-b', '--branches', default=['autoland', 'mozilla-inbound', 'mozilla-central'],
                        help="Branches to gather backout rate on, can be specified "
                             "multiple times.")
    parser.add_argument('-p', '--platform', default='windows10-64',
                        help="platform for results, default is windows10-64")
    parser.add_argument('-c', '--build_type', default='opt',
                        help="build configuration, default is 'opt'.")
    parser.add_argument('--limit', type=int, default=20,
                        help="Maximum number of jobs to return")
    parser.add_argument('--sort-key', type=int, default=2,
                        help="Key to sort on (int, 0-based index)")

    args = parser.parse_args(args)
    query_args = vars(args)
    limit = query_args.pop('limit')

    data = next(run_query('task_durations', **query_args))['data']
    result = []
    for record in data:
        if record[2] is None:
            continue
        record[2] = round(record[2]/60, 2)
        record.append(int(round(record[1] * record[2], 0)))
        result.append(record)

    result = sorted(result, key=lambda k: k[args.sort_key], reverse=True)[:limit]
    result.insert(0, ['Taskname', 'Num Jobs', 'Average Hours', 'Total Hours'])
    return result
