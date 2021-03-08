from dataclasses import dataclass
import numpy as np


@dataclass
class StreamStat:
    init: int = np.inf
    startup: int = np.inf
    last: int = -1
    cum_rebuf: float = 0
    sum_ssim_db: float = 0
    count_ssim_1: int = 0
    count_ssim_sample: int = 0

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
        return self.init == np.inf or self.startup == np.inf or self.last == -1 or self.count_ssim_sample == 0

    @property
    def ssim_db_mean(self):
        return self.sum_ssim_db / self.count_ssim_sample


@dataclass
class GroupStat:
    total_play: float = 0  # in sec
    total_stall: float = 0  # in sec
    num_streams: int = 0
    total_ssim: float = 0  # in db

    @property
    def play_stall_ratio(self):
        return self.total_stall / self.total_play

    @property
    def mean_ssim(self):
        return self.total_ssim / self.num_streams
