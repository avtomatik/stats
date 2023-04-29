
from thesis.src.lib.stockpile import stockpile_can
from thesis.src.lib.transform import transform_agg_sum

from stats.src.can.read import read_can_group_a, read_can_group_b

kwargs = {
    'file_id': 5245628780870031920,
    'skiprows': 3
}
read_can_group_a(**kwargs)

kwargs = {
    'file_id': 7931814471809016759,
    'skiprows': 241
}
kwargs = {
    'file_id': 8448814858763853126,
    'skiprows': 81
}
read_can_group_b(**kwargs)

series_ids = {'v62143969': 36100108}
series_ids = {'v62143990': 36100108}
stockpile_can(series_ids).pipe(transform_agg_sum)
