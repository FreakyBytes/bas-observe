"""
This module contains functions to help vectorise (parts of) the window datamodel
"""
from datetime import datetime, timedelta
import numpy as np

import baos_knx_parser as knx

from . import datamodel


_APCI_KEYS = list(knx.APCI(None)._attr_map.keys())


def vectorise_knx_addr(addr: (knx.KnxAddress, str)):
    if isinstance(addr, str):
        addr = knx.KnxAddress(str=addr, group='/' in addr)

    return np.array([int(b) for b in '{:0>16b}'.format(int(addr))])


# def vectorise_knx_addr_list(addrs):
#     return np.sum([vectorise_knx_addr(addr) for addr in addrs], axis=0) / len(addrs)


def vectorise_knx_addr_dict(addrs):
    vects = []
    size = 0  # amount against which we normalise the vector

    for addr, amount in addrs.items():
        if amount and amount > 0:
            vects.append(vectorise_knx_addr(addr) * amount)
            size += amount

    if size == 0:
        return np.zeros(16)
    else:
        return np.sum(vects, axis=0) / size


def vectorise_apci(apci: knx.APCI):
    return np.array([1 if name == str(apci) else 0 for name in _APCI_KEYS])


# def vectorise_apci_list(apcis):
#     return np.sum([vectorise_apci(apci) for apci in apcis], axis=0) / len(apcis)


def vectorise_apci_dict(apcis):
    vects = []
    size = 0

    for apci, amount in apcis.items():
        if amount and amount > 0:
            vects.append(vectorise_apci(apci) * amount)
            size += amount

    if size == 0:
        return np.zeros(len(_APCI_KEYS))
    else:
        return np.sum(vects, axis=0) / size


def vectorise_time_of_week(dt: datetime):
    # time of week, calculate passed seconds since the start of the week (Monday)
    tow = dt.weekday() * (24 * 60 * 60)
    # add seconds passed in the current day
    tow += (dt.hour * 60 * 60) + (dt.minute * 60) + dt.second
    # normalise against the seconds per week
    return np.array([tow / (7 * 24 * 60 * 60)])


def vectorise_time_of_year(dt: datetime):
    # time of year, calcute passed seconds since the beginning of the year
    toy = dt - datetime(dt.year, 1, 1)
    return np.array([toy.total_seconds() / timedelta(days=365).total_seconds()])


def _priority_to_int(prio: knx.TelegramPriority):
    if prio == knx.TelegramPriority.LOW or prio == 'LOW':
        return 0
    elif prio == knx.TelegramPriority.NORMAL or prio == 'NORMAL':
        return 1
    elif prio == knx.TelegramPriority.URGENT or prio == 'URGENT':
        return 2
    elif prio == knx.TelegramPriority.SYSTEM or prio == 'SYSTEM':
        return 3
    else:
        raise TypeError(f"Unknown KNX Priority {prio} ({type(prio)})")


def vectorise_priority_dict(prios):
    vect = [0] * 4  # init vector with size 4 (0..3)
    size = 0  # size against which we normalise

    for prio, amount in prios.items():
        vect[_priority_to_int(prio)] += amount if amount else 0
        size += amount if amount else 0

    if size == 0:
        return np.zeros(4)
    else:
        return np.array(vect) / size


def vectorise_hop_count(hop_count: int):
    # normalise hop count against maximum (7)
    # actually the max is 6, but hop_count set to 7 means to not reduce it
    # aka. unlimited redirect -> not adviced ;)
    return np.array([hop_count / 7])


def vectorise_hop_count_dict(hop_counts: {}):
    vect = [0] * 7  # init vector with size 7
    size = 0  # size against which we normalise

    for hop_count, amount in hop_counts.items():
        vect[int(hop_count)] += amount if amount else 0
        size += amount if amount else 0

    if size == 0:
        return np.zeros(7)
    else:
        return np.array(vect) / size


def vectorise_payload_length(length: int):
    # normalise against max payload length of extended telegram (255)
    return np.array([length / 255])


def vectorise_payload_length_dict(lengths, buckets=10):
    # buckets = number of dimensions use to represent the length (so it is not overrepresented)
    vect = [0] * buckets
    size = 0

    for length, amount in lengths.items():
        bucket = round((int(length) / 255) * buckets)
        vect[bucket] += amount if amount else 0
        size += amount if amount else 0

    if size == 0:
        return np.zeros(buckets)
    else:
        return np.array(vect) / size


def vectorise_window(window: datamodel.Window):
    try:
        return np.concatenate((                                 # size:
            vectorise_time_of_year(window.start),               # 1
            vectorise_knx_addr_dict(window.src_addr),           # 16
            vectorise_knx_addr_dict(window.dest_addr),          # 16
            vectorise_priority_dict(window.priority),           # 4
            vectorise_hop_count_dict(window.hop_count),         # 7
            vectorise_payload_length_dict(window.length),       # 10 (buckets)
            vectorise_apci_dict(window.apci),                   # 37
        ), axis=0)
    except ValueError as e:
        print(window.to_dict())
        raise e
