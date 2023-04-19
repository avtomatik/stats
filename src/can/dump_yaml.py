from pathlib import Path

import yaml

from stats.src.can.constants import (DATA_CONSTRUCT_CAN_FORMER,
                                     DATA_CONSTRUCT_CAN_FORMER_NOT_USED,
                                     DATA_STATCAN_ARCHIVE)


def dump(path_exp: str, file_name: str, data: dict) -> None:
    with open(Path(path_exp).joinpath(file_name), 'w') as f:
        yaml.dump(data, f, default_flow_style=False)


DIR_EXP = '/home/green-machine/Downloads'

FILE_NAME = 'construct_can_former.yaml'
dump(DIR_EXP, FILE_NAME, DATA_CONSTRUCT_CAN_FORMER)

FILE_NAME = 'construct_can_former_not_used.yaml'
dump(DIR_EXP, FILE_NAME, DATA_CONSTRUCT_CAN_FORMER_NOT_USED)

FILE_NAME = 'statcan_archive.yaml'
dump(DIR_EXP, FILE_NAME, DATA_STATCAN_ARCHIVE)
