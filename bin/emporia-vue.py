#!/usr/bin/env python3
import argparse
import json
import pickle
import datetime
import dateutil
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

Scale = SimpleNamespace(**{  # see strftime
    'S': '1S',
    'M': '1MIN',
    'M15': '15MIN',
    'H': '1H',
    'd': '1D',
    'w': '1W',
    'm': '1MON',
    'y': '1Y',
})

Unit = SimpleNamespace(**{  # Emporia own units; only KWh make sense
    'KWh': 'KilowattHours',
    'USD': 'Dollars',
    'Ah': 'AmpHours',
    'Trees': 'Trees',
    'Gal': 'GallonsOfGas',
    'M': 'MilesDriven',
    'C': 'Carbon',
})

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


def load_cache(config):
    now = datetime.datetime.now().replace(microsecond=0).astimezone(datetime.timezone.utc)

    config.cache_data = dict()

    try:
        if exists(config.cache):
            cache_mtime = (
                datetime.datetime.fromtimestamp(getmtime(config.cache))
                                 .replace(microsecond=0)
                                 .astimezone(datetime.timezone.utc)
            )
            if (now - cache_mtime).seconds < 2700:
                with open(config.cache, 'rb') as f:
                    config.cache_data = pickle.load(f)
                logging.info(f'Using cache: {config.cache}')
            else:
                logging.info(f'Cache expired; not using: {cache_mtime}')
    except (EOFError, pickle.UnpicklingError):
        logging.warning(f'Error loading cache; not using: {config.cache}')

    return now


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
    if 'devices' in config.cache_data and \
            (config.now - config.cache_data['devices']['timestamp']).seconds < 43200:
        logging.info('Using cached device data')
        return

    logging.info('Refreshing device data')
    target = api.devices.format(
        host=api.host
        )
    async with session.get(target) as resp:
        if resp.status not in [200]:
            raise RuntimeError(f'Server returned status: [{resp.status}]')
        else:
            config.cache_data.update({
                'devices': {
                    'devices': (await resp.json())['devices'],
                    'timestamp': config.now,
                    },
                'changed': True,
            })


async def save_compacted_work(config, records, work):
    tasks = set()

    for frame in work:
        if not frame.readings:
            logging.info(f'Nothing to save: {frame}')
            continue
        for index in range(len(frame.readings)):
            frame.readings[index] = {
                'time': frame.readings[index].timestamp,
                'measurement': config.measurment,
                'tags': {
                    'Device': frame.device_gid,
                    'Channel': frame.channel_num,
                    'Description': records[frame.device_gid]
                                          [frame.channel_num].name,
                    'Energy Unit': Unit.KWh,
                    'Scale': Scale.S,
                },
                'fields': {Unit.KWh: frame.readings[index].reading}
            }
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


async def collect_last_saved(config, records):
    tasks = set()

    for device_gid in get_devices(config):
        for channel_num in get_channels(config, device_gid):
            task = asyncio.create_task(
                load_influx_data(config, device_gid, channel_num)
                )
            tasks.add(task)
            task.add_done_callback(tasks.discard)
    return await asyncio.gather(*tasks)


async def load_influx_data(config, device_gid, channel_num):
    async with InfluxDBClientAsync(url=config.url,
                                   token=config.token,
                                   org=config.org) as client:
        try:
            # Emporia keeps 3hr backlog for 1S resolution (technically 3h3m)
            records = await client.query_api().query_stream(
                f'from(bucket: "{config.bucket}")'
                f' |> range(start: {config.backlog})'
                f' |> filter(fn: (r) => r["Energy Unit"] == "{Unit.KWh}")'
                f' |> filter(fn: (r) => r["Device"] == "{device_gid}")'
                f' |> filter(fn: (r) => r["Channel"] == "{channel_num}")'
                f' |> filter(fn: (r) => r["Scale"] == "{Scale.S}")'
                f' |> last()')
            async for record in records:
                pass
        except FluxCsvParserException:
            pass

        value = {
            'device_gid': device_gid,
            'channel_num': channel_num,
            'reading': float('NaN'),
            'planned': list(),
        }

        if 'record' in locals():
            # First read should be 1 second after last recorded timestamp; InfluxDB returns UTC
            timestamp = record['_time'].replace(microsecond=0) + datetime.timedelta(seconds=1)
            value['reading'] = record['_value']
        else:
            timestamp = config.now - datetime.timedelta(hours=3)

        logging.info(f'Starting from {device_gid}:{channel_num}'
                     f' {timestamp} now: {config.now}')

        while timestamp < config.now:
            value['planned'].append(
                SimpleNamespace(**{
                    'start': timestamp,
                    'end': timestamp + config.window - datetime.timedelta(seconds=1)
                })
            )
            timestamp += config.window

        return SimpleNamespace(**value)


def get_devices(config):
    return {
        device['deviceGid']: SimpleNamespace(**{
            'parentDeviceGid': device['parentDeviceGid'],
            'parentChannelNum': device['parentChannelNum'],
            'timeZone': pytz.timezone(device['locationProperties']['timeZone']),
        })
        for device in config.cache_data['devices']['devices']
    }


def get_channels(config, device_gid):
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


def get_planned_work(config, records):
    value = list()

    for device_gid in get_devices(config):
        for channel_num in get_channels(config, device_gid):
            for _ in range(config.load_factor):
                if not records[device_gid][channel_num].planned:
                    continue
                planned = records[device_gid][channel_num].planned.pop(0)
                value.append(SimpleNamespace(**{
                    'device_gid': device_gid,
                    'channel_num': channel_num,
                    'start': planned.start,
                    'end': planned.end,
                    'readings': list(),
                }))

    return value


async def load_planned_work(config, session, planned):
    tasks = set()

    for frame in planned:
        task = asyncio.create_task(
            load_online_data(config, session, frame)
            )
        tasks.add(task)
        task.add_done_callback(tasks.discard)
    await asyncio.gather(*tasks)


async def compact_retrieved_work(config, records, frames):
    for frame in frames:
        readings = list()

        if not frame.readings:
            logging.info(f'No readings for {frame}')
            return

        last_reading = frame.readings.pop()

        while frame.readings:
            current_reading = frame.readings.pop(0)
            if records[frame.device_gid][frame.channel_num].reading != \
                    current_reading.reading:
                records[frame.device_gid][frame.channel_num].reading = \
                    current_reading.reading
                readings.append(current_reading)

        if readings and records[frame.device_gid][frame.channel_num].reading == \
                last_reading.reading:
            readings.pop()

        readings.append(last_reading)
        records[frame.device_gid][frame.channel_num].reading = last_reading.reading

        frame.readings = readings


async def load_online_data(config, session, frame):
    target = api.chart_usage.format(
        host=api.host,
        deviceGid=frame.device_gid,
        channel=frame.channel_num,
        unit=Unit.KWh,
        scale=Scale.S,
        start=frame.start.replace(tzinfo=None).isoformat(),
        end=frame.end.replace(tzinfo=None).isoformat()
        )

    await asyncio.sleep(random.uniform(0.25, 0.75))
    async with session.get(target) as resp:
        body = await resp.json()
        if resp.status not in [200]:
            raise RuntimeError(f'Server says: {resp.status}'
                               f' for {frame.device_gid}:{frame.channel_num} {body}')

    # sanity check
    real_start_time = dateutil.parser.parse(body['firstUsageInstant'])
    if real_start_time != frame.start:
        logging.warning(f'Real start time is different from requested:'
                        f' real: {real_start_time} requested: {frame.start}')

    for reading in body['usageList']:
        if reading is None:
            pass
        elif type(reading) is not float:
            raise RuntimeError(f'Reading is {type(reading)}')
        else:
            frame.readings.append(SimpleNamespace(**{
                'timestamp': real_start_time,
                'reading': reading,
            }))
        real_start_time += datetime.timedelta(seconds=1)

    return frame


async def main(args):
    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))
    config.now = load_cache(config)

    config.window = datetime.timedelta(seconds=args.window)
    config.backlog = args.backlog
    config.load_factor = args.load_factor

    await init_token_cache(config)

    await update_token_cache(config)
    async with aiohttp.ClientSession(
            headers={'authtoken': config.cognito.id_token}
            ) as session:

        await update_devices_cache(config, session)

        records = {
            device: {
                channel_num: channel_data
                for channel_num, channel_data in get_channels(config, device).items()
            }
            for device in get_devices(config).keys()
        }

        for record in await collect_last_saved(config, records):
            records[record.device_gid][record.channel_num].reading = record.reading
            records[record.device_gid][record.channel_num].planned = record.planned

        while True:
            logging.info(f'Running batch of {config.load_factor} frames')

            work = get_planned_work(config, records)
            if not work:
                break

            await load_planned_work(config, session, work)
            await compact_retrieved_work(config, records, work)
            await save_compacted_work(config, records, work)

    await dump_cache(config)


if __name__ == "__main__":
    logging.basicConfig(encoding='utf-8', level=logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        default=f'{expanduser("~")}/emporia-vue/conf/emporia-vue.json')
    parser.add_argument('--window', type=int,
                        default=300)
    parser.add_argument('--load-factor', type=int,
                        default=3)
    parser.add_argument('-b', '--backlog', type=str,
                        default='-3h5m')

    asyncio.run(main(parser.parse_args()))
