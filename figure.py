from matplotlib import pyplot as plt


def plot(result):
    idx = 0
    keys = sorted(list(result.keys()))
    c = ["#78c4d4", "#c19065", "#00af91","#f58634", "#f05454"]
    for i in keys:
        plt.plot(result[i].play_stall_ratio*100, result[i].mean_ssim,
                 "o",color= c[idx], label="{0}".format(i))
        idx += 1
    plt.ylabel("Average SSIM(db)")
    plt.xlabel("Time spent stalled(%)")
    plt.legend(loc='upper left')
    left, right = plt.xlim()
    down, top = plt.ylim()
    plt.xlim(right*1.1, left)
    plt.ylim(0.95*down,top*1.1)
    plt.show()
