from dataclasses import dataclass, field
from matplotlib import pyplot as plt
import numpy as np
import datetime
import pandas as pd

file_covid = "./biweekly-confirmed-covid-19-cases.csv"

def ssim2db(ssim):
    return -10 * np.log10(1 - ssim)


def plot_scatter_mat(df, fields, diff):
    if diff:
        df = df.diff()
        df_joined.dropna(inplace=True)
    n_fields = len(fields)
    fig, axs = plt.subplots(n_fields, n_fields)
    fig.set_size_inches(16, 16)
    fig.suptitle(
        f'Scatter matrix of {"daily variation " if diff else ""}{n_fields} attributes', fontsize=32, y=.95)
    corr_df = df.loc[:, fields].corr()
    for y_idx in range(n_fields):
        for x_idx in range(n_fields):
            curr_ax = axs[y_idx][x_idx]
            curr_ax.ticklabel_format(
                axis="both", style="sci", scilimits=(-2, 2))
            x_field_name = fields[x_idx]
            y_field_name = fields[n_fields - 1 - y_idx]
            curr_ax.scatter(df[x_field_name], df[y_field_name], marker=".")
            if x_idx == 0:
                curr_ax.set_ylabel(y_field_name, fontsize=15)
            if y_idx == n_fields - 1:
                curr_ax.set_xlabel(x_field_name, fontsize=15)
            corr = corr_df.loc[x_field_name, y_field_name]
            curr_ax.set_title(f"corr = {corr:.3f}", y=1.0, pad=-20)
    if diff:
        fig.savefig("corr_diff.png")
    else:
        fig.savefig("corr.png")
    pass


def plot_scatter(df, field_0, field_1):
    df["ssim_db"] = ssim2db(df["ssim"])
    corr_df = df.loc[:, [field_0, field_1]].corr()
    corr = corr_df.loc[field_0, field_1]
    fig, ax = plt.subplots()
    fig.suptitle(
        f'Scatter plot of {field_0} and {field_1} (corr = {corr:.3f})\nover period {df.index[0].date()} to {df.index[-1].date()}')
    ax.set_ylabel(field_0)
    ax.set_xlabel(field_1)
    ax.scatter(df[field_1], df[field_0], marker=".")
    
    fig.savefig(f"[{df.index[0].date()}] scatter chart of {field_0} and {field_1}.png")


def plot_double_line(df, field_0, field_1):
    # Create some mock data
    fig, ax = plt.subplots()
    fig.suptitle(
        f'{field_0} and {field_1} over period {df.index[0].date()} to {df.index[-1].date()}')
    ax.set_ylabel(field_0, color="C0")
    ax.plot(df.index, df[field_0], color="C0")
    ax.tick_params(axis='y', labelcolor="C0")
    ax.tick_params(axis='x', labelrotation=45)
    ax = ax.twinx()  # A second axes that shares the same x-axis

    # we already handled the x-label with ax1
    ax.set_ylabel(field_1, color="C1")
    ax.plot(df.index, df[field_1], color="C1")
    ax.tick_params(axis='y', labelcolor="C1")

    fig.subplots_adjust(bottom=0.18)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    fig.savefig(f"line chart of {field_0} and {field_1}.png")
    pass


if __name__ == "__main__":
    covid_df = pd.read_csv(file_covid)
    covid_df = covid_df[covid_df["Code"] == "USA"]

    out_dir = "out"
    timef = r"%Y-%m-%d"
    first_date = datetime.date(2020, 11, 15)
    days = 111
    # first_date = datetime.date(2020, 7, 27)
    # days = 222
    curr_date = first_date
    days_byweek = 14
    one_day = datetime.timedelta(days=1)
    period_data = {}
    for _ in range(days):
        try:
            file_date = f"{curr_date.strftime(timef)}T11_{(curr_date + one_day).strftime(timef)}T11"
            day_data = np.load(f"out/{file_date}.npy",
                               allow_pickle=True).item()

            scheme_data = []
            for scheme_name in day_data:
                scheme_data.append(day_data[scheme_name].streams)
            scheme_data = pd.concat(scheme_data)

            total_watch_time = scheme_data.watch_time.sum()
            total_stall_time = scheme_data.stall_time.sum()
            total_session = scheme_data.session_id.nunique()
            ssim_index_mean = (
                scheme_data.watch_time * scheme_data.ssim_index_mean).sum() / total_watch_time

            period_data[curr_date] = (
                total_watch_time, total_stall_time, total_session, ssim_index_mean)
        except Exception as e:
            print(e)
        curr_date += one_day

    rolling_period_data = []
    curr_date = first_date
    for _ in range(days - days_byweek + 1):
        [i for i in range(days_byweek)]
        win_data = []
        for i in range(days_byweek):
            day_data = period_data.get(curr_date + i * one_day)
            if day_data is not None:
                win_data.append(day_data)
        win_data = pd.DataFrame(
            win_data, columns=["watch", "stall", "n_session", "ssim"])
        avg_watch_time = win_data.watch.sum() / len(win_data)
        avg_stall_ratio = win_data.stall.sum() / win_data.watch.sum()
        avg_session = win_data.n_session.sum() / len(win_data)
        avg_ssim_index = (
            win_data.watch * win_data.ssim).sum() / win_data.watch.sum()
        curr_last_date = curr_date + (days_byweek - 1) * one_day
        rolling_period_data.append(
            (curr_last_date, avg_watch_time, avg_stall_ratio, avg_session, avg_ssim_index))
        curr_date += one_day

    puffer_df = pd.DataFrame(rolling_period_data, columns=[
                             "Date", "watch_time", "stall_ratio", "num_session", "ssim"])
    puffer_df.set_index(pd.to_datetime(puffer_df['Date']), inplace=True)

    covid_df.set_index(pd.to_datetime(covid_df['Date']), inplace=True)
    df_joined = pd.concat([covid_df, puffer_df], axis=1, join="inner")
    df_joined = df_joined.loc[:, ["Biweekly cases",
                                  "watch_time", "stall_ratio", "num_session", "ssim"]]
    df_joined.dropna(inplace=True)
    # plot_scatter_mat(df_joined, [
    #                  "Biweekly cases", "watch_time", "stall_ratio", "num_session", "ssim"], diff=False)
    # plot_scatter_mat(df_joined, [
    #                  "Biweekly cases", "watch_time", "stall_ratio", "num_session", "ssim"], diff=True)
    # for f1 in ["watch_time", "stall_ratio", "num_session", "ssim"]:
    #     plot_double_line(df_joined, f1, "Biweekly cases")
    # pass

    plot_scatter(df_joined, "num_session", "ssim")
    plot_scatter(df_joined, "num_session", "ssim_db")
    plot_scatter(df_joined, "watch_time", "ssim_db")
    plot_scatter(df_joined, "Biweekly cases", "ssim_db")
    plot_scatter(df_joined, "watch_time", "stall_ratio")
    plot_scatter(df_joined, "num_session", "Biweekly cases")
