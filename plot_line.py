from matplotlib import pyplot as plt
import numpy as np
import stats
import datetime


def npy2plot(data: dict, attr, save_dir):
    attr_name = {
        "stall ratio": "play_stall_ratio",
        "mean ssim": "mean_ssim",
        "number of streams": "num_streams"
    }

    xs = list(data.keys())
    yss = []
    labels = list(data[xs[0]].keys())
    plt.figure()
    for label in labels:
        ys = [data[x][label].__getattribute__(attr_name[attr]) for x in xs]
        yss.append(ys)
        plt.plot(xs, ys, label=label)
    plt.legend()
    plt.gcf().subplots_adjust(bottom=0.18)
    plt.xticks(rotation=45)
    plt.ylabel(attr)
    title_text = f"{attr} of {len(labels)} schemes over {len(xs)} days".title()
    plt.title(title_text)
    plt.savefig(f'{save_dir}/{title_text}.png')
    pass


if __name__ == "__main__":
    out_dir = "out"
    timef = r"%Y-%m-%d"
    curr_date = datetime.date(2021, 1, 1)
    days = 33
    one_day = datetime.timedelta(days=1)
    data = {}
    for _ in range(days):
        file_date = f"{curr_date.strftime(timef)}T11_{(curr_date + one_day).strftime(timef)}T11"
        day_data = np.load(f"out/{file_date}.npy", allow_pickle=True).item()
        data[curr_date] = day_data

        curr_date += one_day
    npy2plot(data, "stall ratio", out_dir)
    npy2plot(data, "mean ssim", out_dir)
    npy2plot(data, "number of streams", out_dir)
