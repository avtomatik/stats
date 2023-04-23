
from thesis.src.lib.pull import transform_agg_sum
from thesis.src.lib.stockpile import stockpile_can
from thesis.src.lib.transform import transform_agg_sum

from stats.src.can.read import read_can_group_a, read_can_group_b

read_can_group_a(7931814471809016759, skiprows=241)
read_can_group_a(8448814858763853126, skiprows=81)
read_can_group_b(5245628780870031920, skiprows=3)
stockpile_can({'v62143969': 36100108}).pipe(transform_agg_sum)
stockpile_can({'v62143990': 36100108}).pipe(transform_agg_sum)
