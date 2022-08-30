#!/usr/bin/env python3
import argparse
import json
import pickle
import datetime
from datetime import timedelta
import dateutil
from dateutil.relativedelta import relativedelta
import os
import pytz
import logging
import random
from pycognito import Cognito
from os.path import exists, getmtime, expanduser

import asyncio
import aiohttp

from types import SimpleNamespace

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.flux_csv_parser import FluxCsvParserException
# from influxdb_client.domain.write_precision import WritePrecision

Scale = {
    'S': '1S',
    'M': '1MIN',
    'M15': '15MIN',
    'H': '1H',
    'd': '1D',
    'w': '1W',
    'm': '1MON',
    'y': '1Y',
}

ScaleDelta = {
    'S': relativedelta(seconds=1),
    'M': relativedelta(minutes=1),
    'M15': relativedelta(minutes=15),
    'H': relativedelta(hours=1),
    'd': relativedelta(days=1),
    'w': relativedelta(weeks=1),
    'm': relativedelta(months=1),
    'y': relativedelta(years=1),
}

Unit = {  # Emporia own units; only KWh make sense
    'KWh': 'KilowattHours',
    'USD': 'Dollars',
    'Ah': 'AmpHours',
    'Trees': 'Trees',
    'Gal': 'GallonsOfGas',
    'M': 'MilesDriven',
    'C': 'Carbon',
}

api = SimpleNamespace(**{
    'host': 'https://api.emporiaenergy.com',
    'devices': '{host}/customers/devices',
    'device_usage': '{host}/AppAPI?apiMethod=getDeviceListUsages'
                    '&deviceGids={deviceGids}'
                    '&instant={instant}Z'
                    '&scale={scale}'
                    '&energyUnit={unit}',
    'chart_usage': '{host}/AppAPI?apiMethod=getChartUsage'
                   '&deviceGid={deviceGid}'
                   '&channel={channel}'
                   '&start={start}Z'
                   '&end={end}Z'
                   '&scale={scale}'
                   '&energyUnit={unit}'
})

aws = SimpleNamespace(**{
    "USER_POOL": "us-east-2_ghlOXVLi1",
    "USER_POOL_REGION": "us-east-2",
    "CLIENT_ID": "4qte47jbstod8apnfic0bunmrq",
})


async def dump_cache(config):
    if 'changed' not in config.cache_data or not config.cache_data['changed']:
        logging.info(f'Cache is not changed, not saving: "{config.cache}"')
        return

    del config.cache_data['changed']

    logging.info(f'Saving cache: {config.cache}')
    with open(config.cache, 'wb') as f:
        pickle.dump(config.cache_data, f)


async def init_token_cache(config):
    if 'token' in config.cache_data:
        logging.debug('tokens are cached in memory, using tokens')
        token = config.cache_data['token']
        config.cognito = Cognito(aws.USER_POOL, aws.CLIENT_ID,
                                 user_pool_region=aws.USER_POOL_REGION,
                                 id_token=token['id_token'],
                                 access_token=token['access_token'],
                                 refresh_token=token['refresh_token'])
    else:
        logging.debug('no cached tokens; using login/password')
        config.cognito = Cognito(aws.USER_POOL, aws.CLIENT_ID,
                                 user_pool_region=aws.USER_POOL_REGION,
                                 username=config.login)
        config.cognito.authenticate(password=config.password)


async def update_token_cache(config):
    config.cognito.check_token()

    data = {
        'access_token': config.cognito.access_token,
        'id_token': config.cognito.id_token,
        'refresh_token': config.cognito.refresh_token,
    }

    if config.cache_data.get('token', None) != data:
        logging.info('Cache tokes updated; setting to changed')
        config.cache_data.update({
            'token': data,
            'changed': True,
        })


async def update_devices_cache(config, session):
    if ('devices' in config.cache_data and
            config.now - config.cache_data['devices']
                                          ['timestamp'] < timedelta(days=1)):
        logging.info('Using cached device data')
        return

    logging.info('Refreshing device data')
    target = api.devices.format(
        host=api.host
        )
    async with session.get(target) as resp:
        body = await resp.json()

    if resp.status not in [200]:
        raise RuntimeError(f'Server says: {resp.status}'
                           f' for devices request; {body}')
    else:
        config.cache_data.update({
            'devices': {
                'devices': body['devices'],
                'timestamp': config.now,
                },
            'changed': True,
        })


async def save_retrieved_data(config, context):
    tasks = set()

    for device_gid, device_data in context.items():
        for channel_num, channel_data in device_data.items():
            for frame in channel_data.processed:
                frame.readings = [
                    {
                        'time': item.timestamp,
                        'measurement': config.measurment,
                        'tags': {
                            'Device': frame.device_gid,
                            'Channel': frame.channel_num,
                            'Description': context[frame.device_gid]
                                                  [frame.channel_num].name,
                            'Energy Unit': Unit[frame.unit],
                            'Scale': Scale[frame.scale],
                        },
                        'fields': {Unit[frame.unit]: item.reading}
                    }
                    for item in frame.readings
                    if item.reading is not None
                ]

                if not frame.readings:
                    logging.info(f'Nothing to save: {frame}')
                    continue

                task = asyncio.create_task(
                    save_influx_data(config, frame.readings)
                    )
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            await asyncio.gather(*tasks)


async def save_influx_data(config, datapoints):
    async with InfluxDBClientAsync(url=config.url,
                                   token=config.token,
                                   org=config.org) as client:
        influx_write_api = client.write_api()
        await influx_write_api.write(bucket=config.bucket,
                                     record=datapoints)


async def populate_from_influxdb_data(config, context, unit, scale):
    tasks = set()

    for device_gid, device_data in context.items():
        for channel_num, channel_data in device_data.items():
            task = asyncio.create_task(
                populate_item_from_influxdb_data(config, context,
                                                 device_gid, channel_num,
                                                 unit, scale)
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)
    await asyncio.gather(*tasks)


async def populate_item_from_influxdb_data(config, context,
                                           device_gid, channel_num,
                                           unit, scale):
    # TODO: check for how long Emporia keeps other intervals
    # Until then keep throwing KeyError as a reminder
    backlog = {
        'S': ('-3h3m', relativedelta(hours=3, minutes=3)),
    }

    async with InfluxDBClientAsync(url=config.url,
                                   token=config.token,
                                   org=config.org) as client:
        try:
            # Emporia keeps 3hr backlog for 1S resolution (technically 3h3m)
            query = (f'from(bucket: "{config.bucket}")'
                     f' |> range(start: {backlog[scale][0]})'
                     f' |> filter(fn: (r) => r["Energy Unit"] == "{Unit[unit]}")'
                     f' |> filter(fn: (r) => r["Device"] == "{device_gid}")'
                     f' |> filter(fn: (r) => r["Channel"] == "{channel_num}")'
                     f' |> filter(fn: (r) => r["Scale"] == "{Scale[scale]}")'
                     f' |> last()')
            logging.debug(query)

            async for record in await client.query_api().query_stream(query):
                pass
        except FluxCsvParserException:
            pass

        context = context[device_gid][channel_num]  # shortcut

        context.planned = list()
        context.processed = list()

        if 'record' in locals():
            timestamp = record['_time'].replace(microsecond=0) + ScaleDelta[scale]
            context.reading = record['_value']
        else:
            timestamp = config.now - backlog[scale][1]
            context.reading = None

        logging.info(f'Starting from {device_gid}:{channel_num} ({unit},{scale})'
                     f' {timestamp} now: {config.now}')

        while timestamp < config.now:
            context.planned.append(
                SimpleNamespace(**{
                    'device_gid': device_gid,
                    'channel_num': channel_num,
                    'unit': unit,
                    'scale': scale,
                    'start': timestamp,
                    'end': timestamp + config.args.window - ScaleDelta[scale]
                })
            )
            timestamp += config.args.window


async def get_devices(config):
    '''
    { 1234: namespace(timeZone='America/Los_Angeles',
                      parentDeviceGid=2345,
                      parentChannelNum='1'), ...  }, ...
    '''
    return {
        device['deviceGid']: SimpleNamespace(**{
            'parentDeviceGid': device['parentDeviceGid'],
            'parentChannelNum': device['parentChannelNum'],
            'timeZone': pytz.timezone(device['locationProperties']['timeZone']),
        })
        for device in config.cache_data['devices']['devices']
    }


async def get_channels(config, device_gid):
    '''
    { '1,2,3': namespace(channelMultiplier=1.0, name='Main'),
      '1': namespace(channelMultiplier=1.0,
                     name='23. master bedroom'), ... }
    '''
    device = next(device
                  for device in config.cache_data['devices']['devices']
                  if device['deviceGid'] == device_gid)

    return {
        item['channelNum']: SimpleNamespace(**{
            'channelMultiplier': item['channelMultiplier'],
            'name': 'Main' if item['name'] is None else item['name'],
        })
        for item in device['channels'] +
        (device['devices'][0]['channels'] if device['devices'] else [])
    }


async def get_planned_work(config, records):
    value = list()

    while True:
        processed = len(value)

        for device_gid, device_data in records.items():
            for channel_num, channel_data in device_data.items():
                if channel_data.planned:
                    item = channel_data.planned.pop(0)

                    channel_data.processed.append(item)  # save for InfluxDB
                    value.append(item)  # to make parallel request to API

                    if len(value) == config.args.max_requests:
                        return value  # PEP 3136 could have helped here

        if processed == len(value):
            return value


async def populate_item_from_online_data(session, item):
    '''
    Populate single `item` from the API; this represents a single API call
    '''
    target = api.chart_usage.format(
        host=api.host,
        deviceGid=item.device_gid,
        channel=item.channel_num,
        unit=Unit[item.unit],
        scale=Scale[item.scale],
        start=item.start.replace(tzinfo=None).isoformat(),
        end=item.end.replace(tzinfo=None).isoformat()
        )

    await asyncio.sleep(random.uniform(0.25, 0.75))
    async with session.get(target) as resp:
        body = await resp.json()

    if resp.status not in [200]:
        raise RuntimeError(f'Server says: {resp.status}'
                           f' for {item.device_gid}:{item.channel_num}; {body}')

    real_start_time = dateutil.parser.parse(body['firstUsageInstant'])
    if real_start_time != item.start:
        logging.warning(f'Real start time is different from the requested:'
                        f' real: {real_start_time} requested: {item.start}')

    item.readings = [
        SimpleNamespace(**{
            'timestamp': real_start_time + ScaleDelta[item.scale] * offset,
            'reading': body['usageList'][offset]
        })
        for offset in range(len(body['usageList']))
    ]

    logging.info(f'Retrieved from the API: {item.device_gid}/{item.channel_num}'
                 f' {item.unit} {item.scale} count={len(body["usageList"])}')


async def populate_from_online_data(session, planned):
    '''
    Retrieve `planned` data from the API; don't return it as a return value
    Update the structures in-place instead, that takes care of sorting, etc.
    '''
    tasks = set()

    for item in planned:
        task = asyncio.create_task(
            populate_item_from_online_data(session, item)
            )
        tasks.add(task)
        task.add_done_callback(tasks.discard)
    await asyncio.gather(*tasks)


async def compact_retrieved_data(context):
    for device_gid, device_data in context.items():
        for channel_num, channel_data in device_data.items():
            for frame in channel_data.processed:
                if not frame.readings:
                    logging.warning(f'No readings for {frame}')
                    continue

                left_ref = context[frame.device_gid][frame.channel_num]

                frame.readings.append(SimpleNamespace(**{
                    'reading': None,
                    'timestamp': None,
                    }))

                for index in range(len(frame.readings)):
                    if left_ref.reading == frame.readings[index].reading:
                        frame.readings[index].reading = None
                    else:
                        if index:
                            frame.readings[index - 1].reading = left_ref.reading
                        if frame.readings[index].timestamp is not None:
                            left_ref.reading = frame.readings[index].reading

                frame.readings.pop()


async def get_config(args):
    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))

    args.window = relativedelta(seconds=args.window)  # TODO calculate maybe?

    config.args = args

    config.now = (
        datetime.datetime.now()
                         .replace(microsecond=0)
                         .astimezone(datetime.timezone.utc)
        )

    config.cache_data = dict()

    try:
        if exists(config.cache):
            cache_mtime = (
                datetime.datetime.fromtimestamp(getmtime(config.cache))
                                 .replace(microsecond=0)
                                 .astimezone(datetime.timezone.utc)
            )
            if (config.now - cache_mtime).seconds < 2700:
                logging.info(f'Loading cache: {config.cache}')
                with open(config.cache, 'rb') as f:
                    config.cache_data = pickle.load(f)
            else:
                logging.info(f'Cache expired; not using: {cache_mtime}')
    except (EOFError, pickle.UnpicklingError):
        logging.warning(f'Error loading cache; not using: {config.cache}')

    return config


async def main(args):
    if args.crontab:
        await asyncio.sleep(random.uniform(20.0, 40.0))

    config = await get_config(args)

    if exists(config.pid):
        pid_mtime = (
            datetime.datetime.fromtimestamp(getmtime(config.pid))
                             .replace(microsecond=0)
                             .astimezone(datetime.timezone.utc)
        )
        if (config.now - pid_mtime).seconds < 300:
            logging.error(f'Fresh PID file exists: {config.pid}')
            return

    with open(config.pid, 'w') as f:
        print(os.getpid(), file=f)

    await init_token_cache(config)

    # RANT: it's possible to pass session around for AIOHTTP but not for InfluxDB??!!
    async with aiohttp.ClientSession(
            headers={'authtoken': config.cognito.id_token}) as session:

        await update_devices_cache(config, session)

        for unit in args.units:
            for scale in args.scales:
                context = {
                    device: {
                        channel_num: channel_data
                        for channel_num, channel_data
                        in (await get_channels(config, device)).items()
                    }
                    for device in await get_devices(config)
                }
                # { 145800: {   '1': namespace(channelMultiplier=1.0,
                #                              name='23. master bedroom'),
                #               '1,2,3': namespace(channelMultiplier=1.0,
                #                                  name='Main'),

                await populate_from_influxdb_data(config, context, unit, scale)

                while True:
                    work = await get_planned_work(config, context)

                    if not work:
                        break

                    await populate_from_online_data(session, work)
                    await asyncio.sleep(random.uniform(3.0, 5.0))

                # all data is a few megabytes, not a problem even for RPi

                await compact_retrieved_data(context)

                await save_retrieved_data(config, context)

    await dump_cache(config)
    os.remove(config.pid)


if __name__ == "__main__":
    logging.basicConfig(encoding='utf-8', level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        default=f'{expanduser("~")}/emporia.json')
    parser.add_argument('--crontab', action='store_true',
                        help='If used from the crontab do the random start delay')
    parser.add_argument('--units', type=str, choices=Unit,
                        nargs='+', default=['KWh', 'Ah'])
    parser.add_argument('--scales', type=str, choices=Scale,
                        nargs='+', default=['S'])
    parser.add_argument('--window', type=int,
                        default=300)
    parser.add_argument('--max-requests', type=int,
                        default=25)

    asyncio.run(main(parser.parse_args()))
