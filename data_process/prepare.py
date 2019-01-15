# coding=utf-8
"""
---
v1.0.0 Initial package

Data processing classes and functions te be used in preparing raw data for further analysis, supported data formats are:

1. Campbell Scientific sonic anemometers raw files
2. Picarro G2103 ammonia analyzer raw files

"""
import json
import os
from dataclasses import dataclass
from itertools import islice
from multiprocessing import Pool

import pandas as pd
import tgadgets.file as fl
import tgadgets.progress as pg


@dataclass
class RawData:
    name: str
    data_format: dict
    file_pattern: dict
    project_path: str
    data_range: pd.date_range
    cores: int
    data_files = []
    data = pd.DataFrame()

    def __post_init__(self):
        self.raw_path = '%s\\01Raw\\%s' % (self.project_path, self.name)
        self.prep_path = '%s\\02Prepared\\%s' % (self.project_path, self.name)
        print("\n>>> Reading [{}] data\n".format(self.name))
        self.data_files = fl.get_files_list(path=self.raw_path, **self.file_pattern)
        for datafile in self.data_files:
            datafile['data_format'] = self.data_format
        self._merge_data()
        print("\n### Finished Reading [{}] data\n".format(self.name))

    def _merge_data(self):
        timer_merge = pg.Timer()
        timer_merge.start("Reading data files for merging", "Initializing")
        with Pool(self.cores) as p:
            timer_merge.switch("Reading with {} processes".format(self.cores))
            datum_async_list = p.map_async(self._read_data_file, self.data_files)
            progress = pg.MpProgress(datum_async_list, 'map')
            progress.show()
        timer_merge.switch("Merging data")
        self.data = pd.concat(datum_async_list.get())
        timer_merge.stop()

    @staticmethod
    def _read_data_file(data_file: dict) -> pd.DataFrame:
        datum = pd.read_csv(data_file['path'], **data_file['data_format'])
        datum.set_index(datum.columns[0], inplace=True)
        return datum

    def __enter__(self):
        print("\n>>> Processing [{}] data\n".format(self.name))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("\n### Finished processing [{}] data\n".format(self.name))


class SonicData(RawData):
    def prepare_data(self):
        split_path = self.prep_path + '\\Split'
        os.makedirs(split_path, exist_ok=True)
        timer_split = pg.Timer()
        timer_split.start("Splitting data files", "Processing data")
        data_and_range_fractions = self._make_data_fractions(self.data, self.data_range, self.cores)
        with Pool(self.cores) as p:
            timer_split.switch('Preparing data for output')
            results = [p.apply_async(self._split_data_fraction, (*data_and_range_fraction, split_path)) for
                       data_and_range_fraction in data_and_range_fractions]
            timer_split.switch('Writing data files')
            progress = pg.MpProgress(results, 'apply')
            progress.show()
            timer_split.stop()

    @staticmethod
    def _make_data_fractions(data: pd.DataFrame, data_range, cores=os.cpu_count()):
        fraction_size, extra = divmod(len(data_range), cores * 8)
        if extra:
            fraction_size += 1
        data_range_iter = iter(data_range)
        while 1:
            fraction = tuple(islice(data_range_iter, fraction_size))
            if not fraction:
                return
            start_time = fraction[0]
            end_time = fraction[-1] + data_range.freq
            data_range_fraction = pd.date_range(fraction[0], fraction[-1], freq=data_range.freq)
            try:
                yield (data[start_time:end_time], data_range_fraction)
            except Exception as e:
                print(e)

    @staticmethod
    def _split_data_fraction(data_fraction: pd.DataFrame, data_range_fraction, split_path: str):
        i = 0
        while i < len(data_range_fraction):
            start_time = data_range_fraction[i]
            end_time = start_time + data_range_fraction.freq
            try:
                data_part = data_fraction[start_time:end_time]
                # convert to "yyyy-mm-dd_HH-MM" for EddyPro input
                part_name = str(start_time.date()) + '_' + str(start_time.time()).replace(':', '-')[:-3]
                file_path = split_path + '\\' + part_name + '.csv'
                data_part.to_csv(file_path, index=False)
            except Exception as e:
                print("%s : %s" % (str(e), start_time))
            i += 1
        return True


class AmmoniaData(RawData):
    def prepare_data(self):
        print(">>> Averaging data")
        self.data = self.data.tshift(8, freq='H')  # Time Zone Change
        data_prep = self.data.resample(self.data_range.freq).mean()
        os.makedirs(self.prep_path, exist_ok=True)
        data_prep.to_csv(self.prep_path + r'\data_averaged.csv')
        print("### Averaging data completed.")


@dataclass
class Project:
    name: str
    path: str
    begin_time: str
    end_time: str
    freq: str
    sonic_config: dict
    ammonia_config: dict
    cores: int = 0

    def __post_init__(self):
        data_range = pd.date_range(self.begin_time, self.end_time, freq=self.freq, closed='left')
        if not self.cores:
            self.cores = os.cpu_count()
        self.sonic_config['project_path'] = self.ammonia_config['project_path'] = self.path
        self.sonic_config['data_range'] = self.ammonia_config['data_range'] = data_range
        self.sonic_config['cores'] = self.ammonia_config['cores'] = self.cores

    def __enter__(self):
        print("\n>>> Processing data from project[{}]\n".format(self.name))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("\n### Finished processing project[{}]\n".format(self.name))


def load_project(config_file):
    with open(config_file, 'r') as config:
        project_config = json.load(config)
    return Project(**project_config)
