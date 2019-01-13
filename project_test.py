# coding=utf-8
from multiprocessing import freeze_support

from data_process.prepare import SonicData, AmmoniaData, load_project

if __name__ == '__main__':
    freeze_support()
    with load_project('test.prj') as test_project:
        # noinspection PyArgumentList
        with SonicData(**test_project.sonic_config) as sonic_data:
            sonic_data.prepare_data()
        # noinspection PyArgumentList
        with AmmoniaData(**test_project.ammonia_config) as ammonia_data:
            ammonia_data.prepare_data()
