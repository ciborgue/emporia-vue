import asyncio
import datetime
import pytest
import pytest_asyncio
import sys
from types import SimpleNamespace
from recursive_diff import recursive_eq

import emporia


fake = SimpleNamespace(**{
    'device': 2052,
    'channel': '1,2,3',
    'timestamp': datetime.datetime.fromtimestamp(0),
})


def default_timestamp():
    value = fake.timestamp
    fake.timestamp += datetime.timedelta(seconds=1)
    return value


def default_records():
    return {
        fake.device: {
            fake.channel: SimpleNamespace(**{
                'reading': None,
            }),
        }
    }

def default_samples():
    return [
        { 'pre':  1.0, 'src': [[ 1.0, 1.0]],
                       'dst': [[None, 1.0]], },
        { 'pre': None, 'src': [[ 1.0, 1.0]],
                       'dst': [[ 1.0, 1.0]], },
        # edge cases
        { 'pre': None, 'src': [],
                       'dst': [], },  # no frames
        { 'pre': None, 'src': [[]],
                       'dst': [[]], },  # one empty frame
        { 'pre': None, 'src': [[], []],
                       'dst': [[], []], },  # two empty frames
        { 'pre': None, 'src': [[ 1.0, 1.0, 1.0, 1.0]],
                       'dst': [[ 1.0,None,None, 1.0]], },
        { 'pre':  1.0, 'src': [[ 1.0, 1.0, 1.0, 1.0]],
                       'dst': [[None,None,None, 1.0]], },
        { 'pre':  1.0, 'src': [[ 1.0, 1.0, 1.0, 2.0]],
                       'dst': [[None,None, 1.0, 2.0]], },
        { 'pre':  1.0, 'src': [[ 1.0, 1.0, 1.0, 1.0],
                               [ 1.0, 1.0, 1.0, 1.0]],
                       'dst': [[None,None,None, 1.0],
                               [None,None,None, 1.0]], },
        { 'pre': None, 'src': [[ 1.0, 1.0, 1.0, 1.0],
                               [ 1.0, 1.0, 1.0, 1.0]],
                       'dst': [[ 1.0,None,None, 1.0],
                               [None,None,None, 1.0]], },
        None,
        { 'pre': None, 'src': [[None]], 'dst': [[]], },  # one frame with one empty reading
        { 'pre': None, 'src': [[None, None]], 'dst': [[]], },  # one frame with two empty readings
        { 'pre': None, 'src': [[None, None, None]], 'dst': [[]], },  # one frame with three empty reading
        { 'pre': None, 'src': [[None],
                               [None]], 'dst': [[], []], },
        { 'pre': None, 'src': [[None, None],
                               [None, None]], 'dst': [[], []], },
        { 'pre': None, 'src': [[None, None, None],
                               [None, None, None],
                               [None, None, None]], 'dst': [[], [], []], },
        # normal cases
        { 'pre': None, 'src': [[1.0]], 'dst': [[1.0]], },
        { 'pre': 1.0,  'src': [[1.0, 1.0]], 'dst': [[1.0]], },
        { 'pre': 2.0,  'src': [[1.0, 1.0]], 'dst': [[1.0, 1.0]], },
        { 'pre': 1.0,  'src': [[1.0, 1.0, 1.0]], 'dst': [[1.0]], },
    ]


@pytest.mark.asyncio
async def test_compact_null_readings():
    for sample in default_samples():
        if sample is None:
            break
        records = default_records()
        records[fake.device][fake.channel].reading = sample['pre']  # update fake data from InfluxDB

        src = [
            SimpleNamespace(**{
                'device_gid': fake.device,
                'channel_num': fake.channel,
                'readings': [
                    SimpleNamespace(**{
                        'timestamp': default_timestamp(),
                        'reading': data[index],
                    })
                    for index in range(len(data))
                ]
            })
            for data in sample['src']
        ]

        dst = await emporia.compact_retrieved_work(records, src)
        dst = [
            [item.reading for item in item.readings]
            for item in dst
        ]
        print(dst, file=sys.stderr)

        assert recursive_eq(dst, sample['dst']) is None
