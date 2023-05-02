
from ....thesis.src.lib.stockpile import stockpile_can
from ....thesis.src.lib.transform import transform_year_sum
from .read import read_can_groupby

file_id = 5245628780870031920
file_id = 7931814471809016759
file_id = 8448814858763853126
read_can_groupby(file_id)


series_ids = {'v62143969': 36100108}
series_ids = {'v62143990': 36100108}
stockpile_can(series_ids).pipe(transform_year_sum)
