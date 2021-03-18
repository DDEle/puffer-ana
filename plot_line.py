from dataclasses import dataclass
from matplotlib import pyplot as plt
import numpy as np
import stats
import datetime
import pandas as pd


def npy2plot(first_date, days, data: dict, window_size, attr, save_dir, ci=False):
    if attr not in ["stall ratio", "mean ssim"]:
        ci = False
    one_day = datetime.timedelta(days=1)
    dates = [first_date + i * one_day for i in range(days)]
    dates_last = dates[window_size - 1:]
    label_set = set()
    for d in dates:
        if data.get(d):
            for l in data[d].keys():
                label_set.add(l)

    labels = sorted(label_set)
    plt.figure()
    c = ["#78c4d4", "#c19065", "#00af91", "#f58634", "#f05454"]
    for il, label in enumerate(labels):
        # color =
        xs = []
        ys = []
        lys = []
        hys = []
        for i in range(len(dates_last)):
            window_data = []
            for d in dates[i: i + window_size]:
                if data.get(d):
                    window_data.append(data[d])
            if len(window_data) == 0:
                continue
            dfs = [d.get(label) and d[label].streams for d in window_data]
            if all(map(lambda x: x is None, dfs)):
                continue
            all_streams = pd.concat(dfs)
            xs.append(dates_last[i])
            if attr == "stall ratio":
                ly, y, hy = stats.stall_ratio_stat(all_streams)
                lys.append(ly)
                ys.append(y)
                hys.append(hy)
            elif attr == "mean ssim":
                ly, y, hy = stats.ssim_stat_db(all_streams)
                lys.append(ly)
                ys.append(y)
                hys.append(hy)
            elif attr == "daily number of streams":
                ys.append(len(all_streams) / len(window_data))
            elif attr == "daily number of users":
                ys.append(all_streams.session_id.nunique() / len(window_data))
            elif attr == "daily total watch hours":
                ys.append(all_streams.watch_time.sum() /
                          len(window_data) / 3600)
            else:
                raise Exception("Invalid attr to plot.")
        plt.plot(xs, ys, label=label)
        if ci:
            plt.fill_between(xs, lys, hys, alpha=.1)
    plt.legend()
    plt.gcf().subplots_adjust(bottom=0.18)
    plt.xticks(rotation=45)
    plt.ylabel(attr)
    title_text = f"{attr.capitalize()} of {len(labels)} schemes over {len(xs)} days\nwith window size {window_size}"
    if ci:
        title_text += " with shaded confidence interval"
    plt.title(title_text)
    save_file_name = title_text.replace("\n", " ")
    plt.savefig(f'{save_dir}/{dates[0]}_{save_file_name}.png')
    pass


if __name__ == "__main__":
    out_dir = "out"
    timef = r"%Y-%m-%d"
    first_date = datetime.date(2020, 7, 27)
    curr_date = first_date
    days = 223
    one_day = datetime.timedelta(days=1)
    data = {}
    for _ in range(days):
        try:
            file_date = f"{curr_date.strftime(timef)}T11_{(curr_date + one_day).strftime(timef)}T11"
            day_data = np.load(f"out/{file_date}.npy",
                               allow_pickle=True).item()
            data[curr_date] = day_data
        except Exception as e:
            print(e)
        curr_date += one_day

    for window_size in [56]:
        npy2plot(first_date, days, data, window_size, "stall ratio", out_dir)
        npy2plot(first_date, days, data, window_size,
                 "stall ratio", out_dir, ci=True)
        npy2plot(first_date, days, data, window_size, "mean ssim", out_dir)
        npy2plot(first_date, days, data, window_size,
                 "mean ssim", out_dir, ci=True)
        npy2plot(first_date, days, data, window_size,
                 "daily number of streams", out_dir)
        npy2plot(first_date, days, data, window_size,
                 "daily number of users", out_dir)
        npy2plot(first_date, days, data, window_size,
                 "daily total watch hours", out_dir)
