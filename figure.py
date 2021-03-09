from matplotlib import pyplot as plt
import numpy as np
import stats
import datetime


def plot(result):
    idx = 0
    keys = sorted(list(result.keys()))
    c = ["#78c4d4", "#c19065", "#00af91", "#f58634", "#f05454"]
    for i in keys:
        plt.plot(result[i].play_stall_ratio*100, result[i].mean_ssim,
                 "o", color=c[idx], label="{0}".format(i))
        idx += 1
    plt.ylabel("Average SSIM(db)")
    plt.xlabel("Time spent stalled(%)")
    plt.legend(loc='upper left')
    left, right = plt.xlim()
    down, top = plt.ylim()
    plt.xlim(right*1.1, left)
    plt.ylim(0.95*down, top*1.1)
    plt.show()


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
