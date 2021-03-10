from dataclasses import dataclass, field
import numpy as np
from collections import defaultdict
import pandas as pd
import scipy.stats as st


def ssim2db(ssim):
    return -10 * np.log10(1 - ssim)


@dataclass
class StreamStat:
    init: int = np.inf
    startup: int = np.inf  # time_at_startup
    startup_rebuf: float = -1  # time_at_startup
    sum_ssim_index: float = 0
    # count_ssim_1: int = 0
    count_ssim_sample: int = 0
    playing: bool = False
    last_play: int = -1  # time_at_last_play
    last_play_cum_rebuf: int = -1  # cum_rebuf_at_last_play
    bad: int = 0  # 1: event_interval>8s; 2: stall>20s; 3: stall_while_playing

    @property
    def startup_delay(self):  # in seconds
        return (self.startup - self.init) * 1e-9

    @property
    def total_play(self):  # in seconds
        return (self.last_play - self.startup) * 1e-9

    @property
    def total_stall(self):  # in seconds
        return self.last_play_cum_rebuf - self.startup_rebuf

    @property
    def invalid(self):
        if self.init == np.inf:  # never init
            return -1
        if self.startup == np.inf:  # never start
            return -2
        if self.count_ssim_sample == 0:
            return -3
        if self.bad:
            return self.bad
        if self.last_play_cum_rebuf == -1 or self.startup_rebuf == -1:
            return -100
        return 0

    @property
    def ssim_index_mean(self):
        return self.sum_ssim_index / self.count_ssim_sample


@dataclass
class GroupStat:
    # ["session_id", "index", "watch_time", "ssim_index_mean", "stall_time"]
    streams: pd.DataFrame = None
    num_streams_bad: int = 0
    bad_reasons: dict = field(default_factory=lambda: defaultdict(int))

    @property
    def total_watch(self):
        return self.streams["watch_time"].sum()

    @property
    def total_stall(self):
        return self.streams["stall_time"].sum()

    @property
    def play_stall_ratio(self):
        total_watch = self.total_watch
        return total_watch and self.total_stall / self.total_watch

    @property
    def mean_ssim(self):
        total_watch = self.total_watch
        return total_watch and (self.streams["watch_time"] * self.streams["ssim_index_mean"]).sum() / self.total_watch

    @property
    def mean_ssim_db(self):
        return ssim2db(self.mean_ssim)

    @property
    def sum_squared_weights(self):
        return (self.streams["watch_time"] ** 2).sum() / self.total_watch ** 2

    @property
    def ssim_stat_db(self):
        mean = self.mean_ssim
        z95 = st.norm.ppf(.975)
        var = (self.streams["watch_time"] *
               (self.streams["ssim_index_mean"] - mean) ** 2).sum() / self.total_watch
        stddev = np.sqrt(var)
        sem = stddev * np.sqrt(self.sum_squared_weights)
        return ssim2db(mean - z95 * sem), ssim2db(mean), ssim2db(mean + z95 * sem)

    @property
    def stall_ratio_stat(self):
        mean = self.play_stall_ratio
        ratios = self.streams["stall_time"] / \
            self.streams["watch_time"]
        z95 = st.norm.ppf(.975)
        var = (self.streams["watch_time"] * (ratios - mean) ** 2).sum() / self.total_watch
        stddev = np.sqrt(var)
        sem = stddev * np.sqrt(self.sum_squared_weights)
        return mean - z95 * sem, mean, mean + z95 * sem
