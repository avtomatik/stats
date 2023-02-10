# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 22:20:54 2021

@author: Alexander Mikhailov
"""


import os

from lib.tools import build_push_data_frame

# =============================================================================
# Capital
# =============================================================================
# =============================================================================
# 16100077-eng.xlsx: NO;
# 16100088-eng.xlsx: NO;
# 16100118-eng.xlsx: NO;
# 34100278-eng.xlsx: NO;
# 34100279-eng.xlsx: NO;
# 36100096-eng.xlsx: YES;
# 36100097-eng.xlsx: NO;
# 36100109-eng.xlsx: NO;
# 36100174-eng.xlsx: NO;
# 36100210-eng.xlsx: NO;
# 36100236-eng.xlsx: YES ALTERNATIVE;
# 36100237-eng.xlsx: YES ALTERNATIVE;
# 36100238-eng.xlsx: NO;
# =============================================================================

# =============================================================================
# Labor
# =============================================================================
# =============================================================================
# 14100027-eng.xlsx: YES;
# 14100036-eng.xlsx: NO;
# 14100221-eng.xlsx: NO;
# 14100235-eng.xlsx: YES;
# 14100238-eng.xlsx: NO;
# 14100242-eng.xlsx: NO;
# 14100243-eng.xlsx: NO;
# 14100259-eng.xlsx: MAYBE;
# 14100265-eng.xlsx: YES;
# 14100355-eng.xlsx: MAYBE;
# 14100392-eng.xlsx: NO;
# 36100489-eng.xlsx: NO;
# =============================================================================

# =============================================================================
# Manufacturing
# =============================================================================
# =============================================================================
# 10100094-eng.xlsx: Capacity utilization rates (Bank of Canada calculated series), seasonally adjusted
# 16100013-eng.xlsx: NO;
# 16100038-eng.xlsx: NO;
# 16100047-eng.xlsx: NO;
# 16100052-eng.xlsx: NO;
# 16100053-eng.xlsx: NO;
# 16100054-eng.xlsx: NO;
# 16100056-eng.xlsx: NO;
# 16100079-eng.xlsx: NO;
# 16100109-eng.xlsx: Industrial capacity utilization rates, by industry
# 16100111-eng.xlsx: Industrial capacity utilization rates, by Standard Industrial Classification, 1980 (SIC)
# 16100117-eng.xlsx: NO;
# 16100119-eng.xlsx: NO;
# 36100207-eng.xlsx: NO;
# 36100208-eng.xlsx: Capital stock: v41713073;
# 36100217-eng.xlsx: NO;
# 36100303-eng.xlsx: NO;
# 36100305-eng.xlsx: NO;
# 36100309-eng.xlsx: NO;
# 36100310-eng.xlsx: NO;
# 36100383-eng.xlsx: NO;
# 36100384-eng.xlsx: NO;
# 36100385-eng.xlsx: YES;
# 36100386-eng.xlsx: YES;
# 36100480-eng.xlsx: Total number of jobs: v111382232;
# 36100488-eng.xlsx: Output, by sector and industry, provincial and territorial: v64602050;
# =============================================================================

BLUEPRINT_CAPITAL = {
    'v90968617': '36100096-eng.zip', 'v90968618': '36100096-eng.zip',
    'v90968619': '36100096-eng.zip', 'v90968620': '36100096-eng.zip',
    'v90968621': '36100096-eng.zip', 'v90971177': '36100096-eng.zip',
    'v90971178': '36100096-eng.zip', 'v90971179': '36100096-eng.zip',
    'v90971180': '36100096-eng.zip', 'v90971181': '36100096-eng.zip',
    'v90973737': '36100096-eng.zip', 'v90973738': '36100096-eng.zip',
    'v90973739': '36100096-eng.zip', 'v90973740': '36100096-eng.zip',
    'v90973741': '36100096-eng.zip', 'v46444563': '36100210-eng.zip',
    'v46444624': '36100210-eng.zip', 'v46444685': '36100210-eng.zip',
    'v46444746': '36100210-eng.zip', 'v46444807': '36100210-eng.zip',
    'v46444929': '36100210-eng.zip', 'v46444990': '36100210-eng.zip',
    'v46445051': '36100210-eng.zip', 'v46445112': '36100210-eng.zip',
    'v46445173': '36100210-eng.zip', 'v46445295': '36100210-eng.zip',
    'v46445356': '36100210-eng.zip', 'v46445417': '36100210-eng.zip',
    'v46445478': '36100210-eng.zip', 'v46445539': '36100210-eng.zip',
    'v46445661': '36100210-eng.zip', 'v46445722': '36100210-eng.zip',
    'v46445783': '36100210-eng.zip', 'v46445844': '36100210-eng.zip',
    'v46445905': '36100210-eng.zip', 'v1071434': '36100236-eng.zip',
    'v1071435': '36100236-eng.zip', 'v1071436': '36100236-eng.zip',
    'v1071437': '36100236-eng.zip', 'v64498363': '36100236-eng.zip',
    'v1119722': '36100236-eng.zip', 'v1119723': '36100236-eng.zip',
    'v1119724': '36100236-eng.zip', 'v1119725': '36100236-eng.zip',
    'v64498371': '36100236-eng.zip', 'v4421025': '36100236-eng.zip',
    'v4421026': '36100236-eng.zip', 'v4421027': '36100236-eng.zip',
    'v4421028': '36100236-eng.zip', 'v64498379': '36100236-eng.zip'
}

BLUEPRINT_LABOUR = {
    'v2523013': '14100027-eng.zip', 'v54027148': '14100221-eng.zip',
    'v54027152': '14100221-eng.zip', 'v74989': '14100235-eng.zip',
    'v1596771': '14100238-eng.zip', '?v78923847': '14100242-eng.zip',
    '?v78923849': '14100242-eng.zip', '?v78923848': '14100242-eng.zip',
    'v78931172': '14100243-eng.zip', 'v78931174': '14100243-eng.zip',
    'v78931173': '14100243-eng.zip', '?v248856': '14100259-eng.zip',
    '?v729388': '14100259-eng.zip', '?v249422': '14100259-eng.zip',
    '?v249984': '14100259-eng.zip', 'v249139': '14100265-eng.zip',
    'v249703': '14100265-eng.zip', 'v250265': '14100265-eng.zip',
    'v2057609': '14100355-eng.zip', 'v123355112': '14100355-eng.zip',
    'v2057818': '14100355-eng.zip', '?v101893670': '14100355-eng.zip',
    '?v101893671': '14100355-eng.zip', '?v101893672': '14100355-eng.zip',
    'v1235071986': '14100392-eng.zip', 'v65521825': '36100489-eng.zip',
    '!v65522120': '36100489-eng.zip', '!v65522415': '36100489-eng.zip'
}

BLUEPRINT_PRODUCT = {
    'v37482': '10100094-eng.zip', 'v535579': '16100053-eng.zip',
    'v535593': '16100053-eng.zip', 'v535663': '16100053-eng.zip',
    'v535677': '16100053-eng.zip', 'v761808': '16100054-eng.zip',
    'v761927': '16100054-eng.zip', 'v4331088': '16100109-eng.zip',
    'v142817': '16100111-eng.zip', 'v21573668': '36100207-eng.zip',
    'v21573686': '36100207-eng.zip', '!v41712954': '36100208-eng.zip',
    'v41713056': '36100208-eng.zip', 'v41713073': '36100208-eng.zip',
    'v41713243': '36100208-eng.zip', 'v86718697': '36100217-eng.zip',
    'v86719219': '36100217-eng.zip', 'v716397': '36100303-eng.zip',
    'v718173': '36100303-eng.zip', 'v719421': '36100305-eng.zip',
    'v41707475': '36100309-eng.zip', '!v41707595': '36100309-eng.zip',
    'v41707775': '36100309-eng.zip', 'v41708195': '36100309-eng.zip',
    'v41708375': '36100309-eng.zip', 'v42189127': '36100310-eng.zip',
    '!v42189231': '36100310-eng.zip', 'v42189387': '36100310-eng.zip',
    'v42189751': '36100310-eng.zip', 'v42189907': '36100310-eng.zip',
    'v248': '36100383-eng.zip', 'v328': '36100383-eng.zip',
    'v394': '36100385-eng.zip', 'v473': '36100385-eng.zip',
    'v11567': '36100386-eng.zip', 'v111382232': '36100480-eng.zip',
    'v64602050': '36100488-eng.zip'
}


def main():

    FILE_NAME = 'stat_can_desc.csv'
    FILE_NAMES = (
        'stat_can_cap.csv',
        'stat_can_lab.csv',
        'stat_can_prd.csv',
    )

    # =========================================================================
    # Construct Excel File from Specification
    # =========================================================================
    for file_name, blueprint in zip(FILE_NAMES, (BLUEPRINT_CAPITAL, BLUEPRINT_LABOUR, BLUEPRINT_PRODUCT)):
        build_push_data_frame(file_name, blueprint)

    # =========================================================================
    # # =============================================================================
    # # Retrieve Series Description
    # # =============================================================================
    # _df = pd.concat(
    #     [
    #         read_temporary(file_name)
    #         for file_name in FILE_NAMES
    #     ],
    #     axis=1
    # )
    #
    # desc = pd.merge(
    #     read_temporary(FILE_NAME),
    #     _df.transpose(),
    #     left_index=True,
    #     right_index=True,
    # )
    # desc.transpose().to_csv(Path(DIR).joinpath(_FILE_NAME))
    # =========================================================================


if __name__ == '__main__':
    main()
