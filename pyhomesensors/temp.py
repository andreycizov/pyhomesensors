import logging
import signal
from argparse import ArgumentParser
from datetime import timedelta, datetime
from multiprocessing.pool import Pool
import os
from time import sleep
from typing import List, NamedTuple, Dict, Optional

import pytz
import requests

PATH = '/json/uncached/{id}/temperature12'


class Measurement(NamedTuple):
    id: str
    time: datetime
    temp: float


def time_now_utc():
    return datetime.now(tz=pytz.utc)


def mp_initializer(*args, **kwargs):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def request_temp(host, id):
    sensor_path = PATH.format(id=id)
    try:
        r, = requests.get(f'{host}{sensor_path}').json()
        t = time_now_utc()
        return Measurement(id, t, r)
    except:
        logging.getLogger(__name__).exception('Host: %s Id: %s', host, id)
        return None


def buffer_write(workdir, path_pattern, meas: List[Measurement]):
    assert len(meas), len(meas)

    files_meas: Dict[str, List[Measurement]] = {}

    for m in meas:
        path = path_pattern.format(t=m.time)

        if not path in files_meas:
            files_meas[path] = []

        files_meas[path].append(m)

    for k, vs in files_meas.items():
        path = os.path.join(workdir, k)

        path_dir, _ = os.path.split(path)

        os.makedirs(path_dir, exist_ok=True)

        with open(path, 'a+') as f_in:
            for v in vs:
                f_in.write('\t'.join((v.id, format(v.time, '%Y-%m-%dT%H:%M:%S.%f'), v.temp)) + '\n')


def buffer_purge(buffer, buffer_size):
    to_write = []

    while len(buffer) > buffer_size:
        to_write += buffer[:buffer_size]
        buffer = buffer[buffer_size:]

    return to_write, buffer


def main(pid: Optional[str], parallel: int, workdir: str, path_pattern: str, delta: timedelta, ids: List[str],
         host: str, buffer_size: int):
    logging.getLogger(__name__).warning('Workdir: %s', workdir)
    logging.getLogger(__name__).warning('Delta: %f', delta.total_seconds())
    logging.getLogger(__name__).warning('Ids: %s', ids)

    if pid:
        while True:
            try:
                with open(pid, 'x') as f_pid:
                    f_pid.write(str(os.getpid()))

                break
            except FileExistsError:
                logging.getLogger(__name__).error('%s pid exists, killing pid', pid)

                with open(pid, 'r') as f_pid:
                    ex_pid = int(f_pid.read())

                try:
                    os.kill(ex_pid, signal.SIGTERM)
                    logging.getLogger(__name__).warning('sent signal to %d', ex_pid)
                except ProcessLookupError:
                    logging.getLogger(__name__).warning('PID %d does not exist', ex_pid)
                    os.unlink(pid)

                sleep(1)

    assert buffer_size > 0

    buffer: List[Measurement] = []

    exiting = False

    def signal_handler(*args, **kwargs):
        nonlocal exiting
        exiting = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    with Pool(parallel, initializer=mp_initializer) as p:
        try:
            while not exiting:
                time_start = time_now_utc()

                r = p.starmap(request_temp, [(host, id) for id in ids])
                r = [x for x in r if x is not None]
                buffer += r

                time_end = time_now_utc()
                time_delta = time_end - time_start

                to_write, buffer = buffer_purge(buffer, buffer_size)

                if len(to_write):
                    buffer_write(workdir, path_pattern, to_write)

                time_sleep_delta = delta - time_delta

                if time_sleep_delta < timedelta():
                    logging.getLogger(__name__).warning('Out of time: %f', -time_sleep_delta.total_seconds())
                else:
                    sleep(time_sleep_delta.total_seconds())
        finally:
            if len(buffer):
                buffer_write(workdir, path_pattern, buffer)

            if pid:
                os.unlink(pid)


def parser():
    parser = ArgumentParser()

    parser.add_argument(
        '-P',
        dest='parallel',
        default=2,
    )

    parser.add_argument(
        '-W',
        dest='workdir',
        default=os.getcwd()
    )

    parser.add_argument(
        '-D',
        dest='delta',
        type=lambda x: timedelta(seconds=float(x)),
        default=timedelta(seconds=5),
    )

    parser.add_argument(
        '--pid',
        dest='pid',
        type=str,
        default=None
    )

    parser.add_argument(
        '-B',
        dest='buffer_size',
        type=int,
        default=100,
    )

    parser.add_argument(
        '--path',
        dest='path_pattern',
        type=str,
        default='./{t:%Y}-{t:%m}-{t:%d}.tsv',
    )

    parser.add_argument(
        '--id',
        dest='ids',
        action='append',
        default=[],
    )

    parser.add_argument(
        '--host',
        dest='host',
        default='http://127.0.0.1:2121'
    )

    return parser


if __name__ == '__main__':
    main(**vars(parser().parse_args()))
