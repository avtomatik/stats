
from thesis.src.lib.stockpile import stockpile_can
from thesis.src.lib.transform import transform_year_sum

from stats.src.can.constants import CAN_GROSS_FIXED_CAPITAL_FORMATION
from stats.src.can.read import read_can_groupby

file_id = 5245628780870031920
file_id = 7931814471809016759
file_id = 8448814858763853126
read_can_groupby(file_id)


for series_id in CAN_GROSS_FIXED_CAPITAL_FORMATION:
    print(
        stockpile_can(
            {series_id['series_id']: series_id['table']}
        ).pipe(transform_year_sum)
    )
