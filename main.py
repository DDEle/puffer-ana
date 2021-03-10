import json
import time
import utils
import stats

import numpy as np
import pandas as pd
from collections import defaultdict
import figure
import wget
import datetime
import os
from pathlib import Path

t_start = time.time()


def print_time(process=None):
    print((f"[{process}] " if process else "") +
          f"time used: ", time.time() - t_start)


# DATE = "2021-02-08T11_2021-02-09T11"

# CLIENT_BUFFER = f"data/client_buffer_{DATE}.csv"
# EXPT_SETTINGS = f"data/{DATE}_logs_expt_settings"
# SSIM = f"data/ssim_{DATE}.csv"
# VIDEO_ACKED = f"data/video_acked_{DATE}.csv"
# VIDEO_SENT = f"data/video_sent_{DATE}.csv"
# SCHEME_STAT_NPY = f"data/scheme_stat_{DATE}.npy"
STREAM_IDX = ["session_id", "index"]


def ssim2db(ssim):
    return -10 * np.log10(1 - ssim)


def ana_client_buffer(f_name, stream_data):
    """ Fill in init / startup / last / cum_rebuf
    """
    last_chunk_time = defaultdict(lambda: np.nan)
    for df in pd.read_csv(f_name, sep=',', chunksize=1_000_000):
        # init of each stream
        for stream_id, row in df[df["event"] == "init"].loc[:, ("time (ns GMT)", *STREAM_IDX)].groupby(
                STREAM_IDX).agg("min").iterrows():
            stream_data[stream_id].init = min(
                stream_data[stream_id].init, row["time (ns GMT)"])

        # event_interval>8s
        for stream_id, grouped in df.groupby(STREAM_IDX):
            last_row_time = grouped.iloc[-1, grouped.columns.get_loc(
                "time (ns GMT)")]
            grouped = grouped.shift()
            grouped.iloc[0, grouped.columns.get_loc(
                "time (ns GMT)")] = last_chunk_time[stream_id]
            long_event_interval = (grouped["time (ns GMT)"].diff() > 8e9).any()
            if long_event_interval:
                stream_data[stream_id].bad = 1
            last_chunk_time[stream_id] = last_row_time

        for stream_id, grouped in df.groupby(STREAM_IDX):
            # -1 to get the last event
            sStat = stream_data[stream_id]
            key_play_event = grouped.loc[grouped.shift(
                -1)["event"] != grouped["event"]]
            for e in key_play_event.iloc:
                if e["event"] == "startup":
                    sStat.startup = e["time (ns GMT)"]
                    sStat.startup_rebuf = e["cum_rebuf"]
                    sStat.playing = True
                    sStat.last_play = e["time (ns GMT)"]
                    sStat.last_play_cum_rebuf = e["cum_rebuf"]
                elif e["event"] == "rebuffer":
                    sStat.playing = False
                elif e["event"] == "play":
                    sStat.playing = True
                    sStat.last_play = e["time (ns GMT)"]
                    sStat.last_play_cum_rebuf = e["cum_rebuf"]
                elif e["event"] == "timer":
                    if sStat.playing == True:
                        sStat.last_play = e["time (ns GMT)"]
                        sStat.last_play_cum_rebuf = e["cum_rebuf"]
                # do nothing for init event

        print_time("ana_client_buffer")


def ana_video_sent(f_name, stream_data):
    for df in pd.read_csv(f_name, sep=',', chunksize=1_000_000):
        # df["ssim_db"] = df["ssim_index"].mask(
        #     ~df['ssim_index'].between(0, 0.99999))
        # df["ssim_db"] = ssim2db(df.loc[:, "ssim_db"])
        # df["ssim_1"] = df["ssim_index"] == 1
        # for stream_id, row in df.groupby(STREAM_IDX).agg({"ssim_db": ["sum", "count"], "ssim_1": "sum"}).iterrows():
        #     stream_data[stream_id].sum_ssim_db += row.ssim_db['sum']
        #     stream_data[stream_id].count_ssim_sample += row.ssim_db['count']
        #     stream_data[stream_id].count_ssim_1 += row.ssim_1["sum"]

        for stream_id, row in df[df["ssim_index"].between(0, 0.99999)].groupby(STREAM_IDX).agg({"ssim_index": ["sum", "count"]}).iterrows():
            stream_data[stream_id].sum_ssim_index += row.ssim_index['sum']
            stream_data[stream_id].count_ssim_sample += row.ssim_index['count']

        print_time("ana_video_sent")


def get_stream_exp_id_map(f_name):
    ret = {}
    for df in pd.read_csv(f_name, sep=',', chunksize=1_000_000):
        for stream_id, row in df.loc[:, ("expt_id", *STREAM_IDX)].groupby(STREAM_IDX).agg(
                ["nunique", "first"]).iterrows():
            assert row.expt_id["nunique"] == 1
            ret[stream_id] = row.expt_id["first"]
        print_time("get expid map")
    return ret


def stream2scheme(stream_stats, video_sent_file, setting_file):
    expt_set = utils.get_expt_settings(setting_file)
    stream_exp_id_map = get_stream_exp_id_map(video_sent_file)
    group_stat = defaultdict(stats.GroupStat)
    group_good_data = defaultdict(list)
    for stream_id in stream_stats:
        expt_id = stream_exp_id_map.get(stream_id)
        if expt_id is None:
            break
        group_name = expt_set[expt_id].get("group")
        curr_steam = stream_stats[stream_id]
        if not curr_steam.invalid:
            group_good_data[group_name].append((  # ["session_id", "index", "watch_time", "ssim_index_mean", "stall_time"]
                *stream_id,
                curr_steam.total_play,
                curr_steam.ssim_index_mean,
                curr_steam.total_stall
            ))
        else:
            group_stat[group_name].num_streams_bad += 1
            group_stat[group_name].bad_reasons[curr_steam.invalid] += 1

    for k in group_stat:
        col_names = ["session_id", "index", "watch_time",
                     "ssim_index_mean", "stall_time"]
        g = group_stat[k]
        g.streams = pd.DataFrame(group_good_data[k], columns=col_names)
        print(k)
        print("  ", f"{g.play_stall_ratio * 100:.4f}%")
        print("  ", f"{g.mean_ssim:.2f}")
    
    print_time("result printed")
    return group_stat


def main():
    root = "data"
    Path(root).mkdir(parents=True, exist_ok=True)
    setting_file = "2021-03-06T11_2021-03-07T11-logs-expt_settings"
    timef = r"%Y-%m-%d"
    one_day = datetime.timedelta(days=1)

    start_date = datetime.date(2021, 1, 1)
    for _ in range(60):
        try:
            need_files = ["video_sent", "ssim", "client_buffer"]
            file_date = f"{start_date.strftime(timef)}T11_{(start_date + one_day).strftime(timef)}T11"
            print_time(file_date)

            for f in need_files:
                url = f'https://storage.googleapis.com/puffer-data-release/{file_date}/{f}_{file_date}.csv'
                f_name = f'{root}/{f}_{file_date}.csv'
                if not os.path.exists(f_name):
                    wget.download(url, f_name)
                    print_time(f'{root}/{f}_{file_date}.csv downloaded')

            stream_data = defaultdict(stats.StreamStat)

            ana_client_buffer(
                f"{root}/client_buffer_{file_date}.csv", stream_data)
            ana_video_sent(f"{root}/video_sent_{file_date}.csv", stream_data)

            group_stat = stream2scheme(
                stream_data, f"{root}/video_sent_{file_date}.csv", setting_file)

            Path("out").mkdir(parents=True, exist_ok=True)
            np.save(f"out/{file_date}.npy", group_stat)

            figure.plot(group_stat)
        finally:
            try:
                for f in need_files:
                    os.remove(f'{root}/{f}_{file_date}.csv')
            except:
                pass
            start_date += one_day


if __name__ == '__main__':
    main()
