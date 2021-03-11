from matplotlib import pyplot as plt
import numpy as np
import stats
import datetime


def plot(result):
    keys = sorted(list(result.keys()))
    c = ["#78c4d4", "#c19065", "#00af91", "#f58634", "#f05454"]

    for i, k in enumerate(keys):
        yl, y, _ = result[k].ssim_stat_db
        xl, x, xh = result[k].stall_ratio_stat
        xl = max(xl, 0)
        xerr = np.array([[x - xl], [xh - x]]) * 100
        plt.errorbar(x*100, y, xerr=xerr, yerr=y - yl,
                     color=c[i] if i < len(c) else None, fmt="o", label=k)
    plt.ylabel("Average SSIM(db)")
    plt.xlabel("Time spent stalled(%)")
    plt.legend(loc='upper left')
    left, right = plt.xlim()
    # down, top = plt.ylim()
    plt.xlim(right, left)
    # plt.ylim(0.95*down, top*1.1)
    plt.show()


if __name__ == "__main__":
    out_dir = "out"
    timef = r"%Y-%m-%d"
    curr_date = datetime.date(2021, 1, 1)
    one_day = datetime.timedelta(days=1)
    file_date = f"{curr_date.strftime(timef)}T11_{(curr_date + one_day).strftime(timef)}T11"
    day_data = np.load(f"out/{file_date}.npy", allow_pickle=True).item()
    plot(day_data)
