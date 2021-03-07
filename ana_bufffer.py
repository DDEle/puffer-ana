import json
import time
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd
import numpy as np

DATE = "2021-02-08T11_2021-02-09T11"

CLIENT_BUFFER = f"data/client_buffer_{DATE}.csv"
EXPT_SETTINGS = f"data/{DATE}_logs_expt_settings"
SSIM = f"data/ssim_{DATE}.csv"
VIDEO_ACKED = f"data/video_acked_{DATE}.csv"
VIDEO_SENT = f"data/video_sent_{DATE}.csv"
STREAM_IDX_LABEL = ["session_id", "index"]

t_start = time.time()


def print_time(process=None):
    print((f"[{process}] " if process else "") + f"time used: ", time.time() - t_start)


@dataclass
class StreamStat:
    init: int = np.inf
    startup: int = np.inf
    last: int = -1
    cum_rebuf: int = 0

    @property
    def startup_delay(self):  # in seconds
        return (self.startup - self.init) * 1e-9

    @property
    def total_play(self):  # in seconds
        return (self.last - self.startup) * 1e-9

    @property
    def total_stall(self):  # in seconds
        return self.cum_rebuf - self.startup_delay

    @property
    def invalid(self):
        return self.init == np.inf or self.startup == np.inf or self.last == -1


@dataclass
class GroupStat:
    total_play: int = 0  # in sec
    total_stall: int = 0  # in sec

    @property
    def play_stall_ratio(self):
        return self.total_stall / self.total_play


def get_stream_exp_id_map():
    ret = {}
    for df in pd.read_csv(VIDEO_SENT, sep=',', chunksize=1_000_000):
        for stream_id, row in df.loc[:, ("expt_id", *STREAM_IDX_LABEL)].groupby(STREAM_IDX_LABEL).agg(
                ["nunique", "first"]).iterrows():
            assert row.expt_id["nunique"] == 1
            ret[stream_id] = row.expt_id["first"]
        print_time("get expid map")
    return ret
    # ret_df = pd.DataFrame(ret.items())
    # ret_df[STREAM_IDX_LABEL] = pd.DataFrame(ret_df[0].tolist(), index=ret_df.index)
    # ret_df = ret_df.loc[:, [*STREAM_IDX_LABEL, 1]]
    # ret_df.columns = [*STREAM_IDX_LABEL, "expt_id"]
    # return ret_df


def get_expt_settings(f_name):
    expt_settings = {}
    with open(f_name) as f:
        ls = f.readlines()

    for line in ls:
        idx, s = line.split(" ", 1)
        doc = json.loads(s)

        name = doc.get("abr_name")
        if not name:
            name = doc.get("abr")
        group = f"{name}/{doc.get('cc')}"
        doc["group"] = group
        expt_settings[int(idx)] = doc
    return expt_settings


expt_set = get_expt_settings(EXPT_SETTINGS)

stream_exp_id_map = get_stream_exp_id_map()

"""
CLIENT_BUFFER:
    time (ns GMT)
    session_id
    index
    expt_id
    channel
    event
    buffer
    cum_rebuf
"""
stream_data = defaultdict(StreamStat)
for df in pd.read_csv(CLIENT_BUFFER, sep=',', chunksize=1_000_000):
    # init of each stream
    for stream_id, row in df[df["event"] == "init"].loc[:, ("time (ns GMT)", *STREAM_IDX_LABEL)].groupby(
            STREAM_IDX_LABEL).agg("min").iterrows():
        stream_data[stream_id].init = min(stream_data[stream_id].init, row["time (ns GMT)"])

    # start of each stream
    for stream_id, row in df[df["event"] == "startup"].loc[:, ("time (ns GMT)", *STREAM_IDX_LABEL)].groupby(
            STREAM_IDX_LABEL).agg("min").iterrows():
        stream_data[stream_id].startup = min(stream_data[stream_id].startup, row["time (ns GMT)"])

    # last message of each stream
    for stream_id, row in df.loc[:, ("time (ns GMT)", *STREAM_IDX_LABEL)].groupby(
            STREAM_IDX_LABEL).agg("max").iterrows():
        stream_data[stream_id].last = max(stream_data[stream_id].last, row["time (ns GMT)"])

    # cum_rebuf of each stream
    for stream_id, row in df.loc[:, ("cum_rebuf", *STREAM_IDX_LABEL)].groupby(
            STREAM_IDX_LABEL).agg("max").iterrows():
        stream_data[stream_id].cum_rebuf = max(stream_data[stream_id].cum_rebuf, row["cum_rebuf"])

    print_time()

total_play = 0
total_stall = 0
for stream_id in stream_data:
    if not stream_data[stream_id].invalid:
        total_play += stream_data[stream_id].total_play
        total_stall += stream_data[stream_id].total_stall

group_stat = defaultdict(GroupStat)
for stream_id in stream_data:
    expt_id = stream_exp_id_map[stream_id]
    group_name = expt_set[expt_id].get("group")
    if not stream_data[stream_id].invalid:
        group_stat[group_name].total_play += stream_data[stream_id].total_play
        group_stat[group_name].total_stall += stream_data[stream_id].total_stall

for k in group_stat:
    g = group_stat[k]
    print(k)
    # print("  ", g.total_play)
    # print("  ", g.total_stall)
    print("  ", f"{g.play_stall_ratio * 100}%")
