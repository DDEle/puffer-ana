from matplotlib import pyplot as plt
import numpy as np
import stats
import datetime
import pandas as pd


def plot(period):  # [(day, {scheme_name: GroupStat})]
    scheme_set = None
    for _, date_data in period:
        if scheme_set is None:
            prev_len = 0
            scheme_set = set(date_data.keys())
        else:
            prev_len = len(scheme_set)
            scheme_set = scheme_set.intersection(set(date_data.keys()))
        if prev_len != len(scheme_set):
            prev_len = len(scheme_set)  # used for breakpoint

    keys = sorted(scheme_set)
    # keys = [k for k in keys if k in [
    #     "mpc/bbr",
    #     "robust_mpc/bbr",
    #     "linear_bba/bbr",
    #     "pensieve/bbr",
    #     "puffer_ttp_cl/bbr",
    # ]]

    # cubic
    # keys = [k for k in keys if k in [
    #     "linear_bba/bbr",
    #     "linear_bba/cubic",
    #     "pensieve/bbr",
    #     "pensieve/cubic",
    #     "puffer_ttp_cl/bbr",
    #     "puffer_ttp_cl/cubic",
    # ]]
    all_schemes_streams = []

    # c = ["#78c4d4", "#c19065", "#00af91", "#f58634", "#f05454"]
    c = ["#2ca02c", "#1f77b4", "#9467bd", "#d62728", "#8c564b"]
    # c = []



    for i, k in enumerate(keys):
        # if k == "puffer_ttp_cl/bbr":
        #     continue
        all_streams = pd.concat([d[k].streams for (_, d) in period])
        all_schemes_streams.append(all_streams)
        yl, y, yh = stats.ssim_stat_db(all_streams)
        xl, x, xh = stats.stall_ratio_stat(all_streams)
        xl = max(xl, 0)
        print(f"{k}:")
        print(f"  ssim:  {yl:.2f} {y:.2f} {yh:.2f}")
        print(f"  stall: {xl:.3f} {x:.3f} {xh:.3f}")
        xerr = np.array([[x - xl], [xh - x]]) * 100
        plt.errorbar(x*100, y, xerr=xerr, yerr=y - yl,
                     color=c[i] if i < len(c) else None, fmt="o", label=k)
    all_schemes_streams = pd.concat(all_schemes_streams)
    title_text = f"QoE of {len(keys)} schemes"
    if len(period) == 1:
        title_text += f", {period[0][0]}"
    else:
        title_text += f" of {(period[-1][0] - period[0][0]).days + 1} days, "
        title_text += f"{period[0][0]} : {period[-1][0]}"
    title_text += "\n"
    title_text += f"({len(all_schemes_streams)} streams, "
    title_text += f"{all_schemes_streams.watch_time.sum() / 3600:.0f} stream hours, "
    title_text += f"{all_schemes_streams.session_id.nunique()} users)"
    print(title_text)

    # plt.xlim(0, 1.6)  # for pansieve ...
    # plt.ylim(15, 18)  # for pansieve ...
    # plt.xlim(0, .4)  # tmp
    # plt.ylim(15.5, 17)  # tmp

    plt.xlim(0, 1.3)  # for bola ...
    plt.ylim(16, 18)  # for bola ...
    plt.ylabel("Average SSIM(db)")
    plt.xlabel("Time spent stalled(%)")
    plt.legend(loc='upper left')
    plt.title(title_text)
    plt.gca().invert_xaxis()
    plt.savefig(f'out/{period[0][0]}_cross.png')
    plt.show()


def plot_bar(period):
    scheme_set = None
    for _, date_data in period:
        if scheme_set is None:
            prev_len = 0
            scheme_set = set(date_data.keys())
        else:
            prev_len = len(scheme_set)
            scheme_set = scheme_set.intersection(set(date_data.keys()))
        if prev_len != len(scheme_set):
            prev_len = len(scheme_set)  # used for breakpoint

    keys = sorted(scheme_set)
    keys = [k for k in keys if k in [
        "mpc/bbr",
        "robust_mpc/bbr",
        "linear_bba/bbr",
        "pensieve/bbr",
        "puffer_ttp_cl/bbr",
    ]]
    schemes_streams = {}

    for i, k in enumerate(keys):
        all_streams = pd.concat([d[k].streams for (_, d) in period])
        schemes_streams[k] = all_streams

    title_text = f"Average watch time of {len(keys)} schemes"
    if len(period) == 1:
        title_text += f", {period[0][0]}"
    else:
        title_text += f" of {(period[-1][0] - period[0][0]).days + 1} days, "
        title_text += f"{period[0][0]} : {period[-1][0]}"
    avg_time = [schemes_streams[k].watch_time.sum() / 60 /
                schemes_streams[k].session_id.nunique() for k in keys]
    y_pos = list(range(len(keys)))
    plt.bar(y_pos, avg_time)
    plt.ylim(25, 35)
    plt.xticks(y_pos, keys, rotation=30)
    plt.gcf().subplots_adjust(bottom=0.18)
    plt.ylabel("Average watch time (minutes)")
    plt.title(title_text)
    for i, data in enumerate(avg_time):
        plt.text(i - .2, data + .1, s=f"{data:.2f}")

    plt.savefig(f'out/{period[0][0]}_bar_avg_time.png')
    plt.show()


def summary(period):
    scheme_set = None
    for _, date_data in period:
        if scheme_set is None:
            prev_len = 0
            scheme_set = set(date_data.keys())
        else:
            prev_len = len(scheme_set)
            scheme_set = scheme_set.intersection(set(date_data.keys()))
        if prev_len != len(scheme_set):
            prev_len = len(scheme_set)  # used for breakpoint

    keys = sorted(scheme_set)
    # keys = [k for k in keys if k in [
    #     "mpc/bbr",
    #     "robust_mpc/bbr",
    #     "linear_bba/bbr",
    #     "pensieve/bbr",
    #     "puffer_ttp_cl/bbr",
    # ]]

    keys = [k for k in keys if k in [
        "linear_bba/bbr",
        "linear_bba/cubic",
        "pensieve/bbr",
        "pensieve/cubic",
        "puffer_ttp_cl/bbr",
        "puffer_ttp_cl/cubic",
    ]]
    schemes_streams = {}

    for i, k in enumerate(keys):
        all_streams = pd.concat([d[k].streams for (_, d) in period])
        schemes_streams[k] = all_streams
    schemes_streams = pd.concat([schemes_streams[k] for k in schemes_streams])
    overall_stall_ratio = schemes_streams.stall_time.sum() / \
        schemes_streams.watch_time.sum()
    data_with_stall = schemes_streams[schemes_streams.stall_time > 0]
    overall_stall_ratio_with_stall = data_with_stall.stall_time.sum() / \
        data_with_stall.watch_time.sum()
    stall_stream_persent = len(data_with_stall) / len(schemes_streams)
    num_streams = len(schemes_streams)
    total_watch_year = schemes_streams.watch_time.sum() / 365 / 24 / 3600


    summary_text = "\n\n========== Data Summary ==========\n"
    summary_text += f"{keys}\n"
    summary_text += f"Start date: {period[0][0]}\n"
    summary_text += f"End date: {period[-1][0]}\n"
    summary_text += f"Overall stall ratio: {overall_stall_ratio * 100:.4f}%\n"
    summary_text += f"Overall stall ratio over stalled streams: {overall_stall_ratio_with_stall * 100:.4f}%\n"
    summary_text += f"Ratio of streams with any stall: {stall_stream_persent * 100:.4f}%\n"
    summary_text += f"Number of streams: {num_streams}\n"
    summary_text += f"Total watch time: {total_watch_year:.2f} years\n"
    summary_text += "======== Data Summary End ========\n"



    print(summary_text)


if __name__ == "__main__":
    out_dir = "out"
    timef = r"%Y-%m-%d"
    curr_date = datetime.date(2019, 1, 26)
    # curr_date = datetime.date(2020, 7, 27)
    num_days = 264
    one_day = datetime.timedelta(days=1)
    period_data = []
    for _ in range(num_days):
        try:
            if curr_date in [
                    datetime.date(2019, 2, 1),
                    datetime.date(2019, 3, 26),
                    datetime.date(2019, 4, 9),
                    datetime.date(2019, 7, 2),
                    *[datetime.date(2019, 8, 8) + i *
                      one_day for i in range(22)],
                    datetime.date(2019, 8, 30),
                    datetime.date(2019, 9, 7),
                    datetime.date(2019, 9, 26),
                    datetime.date(2020, 9, 15),
                    datetime.date(2020, 9, 27),
                    datetime.date(2020, 10, 12),
                    datetime.date(2020, 10, 19),
                    # high ci of stall ratio of bola v1
                    datetime.date(2020, 10, 28),
                    datetime.date(2020, 11, 4),
                    datetime.date(2020, 11, 16),
                    datetime.date(2020, 11, 24),
                    datetime.date(2020, 12, 1),
                    datetime.date(2020, 12, 25),
                    datetime.date(2020, 8, 5)]:
                curr_date += one_day
                continue
            file_date = f"{curr_date.strftime(timef)}T11_{(curr_date + one_day).strftime(timef)}T11"
            day_data = np.load(f"out/{file_date}.npy",
                               allow_pickle=True).item()
            period_data.append((curr_date, day_data))
        except Exception as e:
            print(e)
        curr_date += one_day
    # plot(period_data)
    plot_bar(period_data)
    # summary(period_data)
