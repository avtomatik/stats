# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


from lib.tools import construct_usa_hist_deflator


def main():
    DIR = '/media/green-machine/KINGSTON'
    DIR_EXPORT = '/media/green-machine/KINGSTON'

    # =============================================================================
    # CALLS = (
    #     # =========================================================================
    #     # construct_usa_hist_deflator(
    #     #     {
    #     #         'CDT2S1': 'dataset_usa_cobb-douglas.zip',
    #     #         'CDT2S3': 'dataset_usa_cobb-douglas.zip'
    #     #     }
    #     # ),
    #     # construct_usa_hist_deflator(
    #     #     {'P0107': 'dataset_uscb.zip', 'P0110': 'dataset_uscb.zip'}
    #     # ),
    #     # =========================================================================
    #     collect_can_price_a,
    #     collect_can_price_b,
    # )

    # data = pd.concat([call() for call in CALLS], axis=1)
    # data['mean'] = data.mean(axis=1)
    # data['cum_mean'] = df.iloc[:, -1].add(1).cumprod()
    # data = data.div(data.loc[2012])
    # =========================================================================

    # # =============================================================================
    # # Product
    # # =============================================================================
    # # =============================================================================
    # # {'v21573668': 36100207} # Not Useful: Real Gross Domestic Product
    # # {'v142817': 16100111} # Not Useful: Capacity Utilization
    # # {'v37482': 10100094} # Not Useful: Capacity Utilization
    # # {'v4331088': 16100109} # Not Useful: Capacity Utilization
    # # {'v41713056': 36100208} # Not Useful: Capital Input
    # # {'v41713073': 36100208} # Not Useful: Capital Input
    # # {'v41707775': 36100309} # Not Useful: Capital Input
    # # {'v42189387': 36100310} # Not Useful: Capital Input
    # # =============================================================================
    # df = DataFrame()
    # combined = DataFrame()

    # # =============================================================================
    # # Capital cost
    # # =============================================================================
    # SERIES_IDS = (
    #     'v41713243': 36100208,
    #     'v41708375': 36100309,
    #     'v42189907': 36100310,
    # )
    # combined = stockpile_can(SERIES_IDS).pipe(mean_series_id)
    # combined = combined.div(combined.loc[1997]).mul(100)
    # combined['mean_comb'] = combined.mean(axis=1)
    # combined = combined.iloc[:, [-1]]
    # df = pd.concat([df, combined], axis=1)
    # # combined.plot(grid=True).get_figure().savefig('view.pdf', format='pdf', dpi=900)
    # # combined.to_csv(Path(DIR_EXPORT).joinpath('df.csv'))

    FILE_NAME = 'stat_can_cap.csv'
    # data = read_temporary(FILE_NAME)
    # data = data.div(data.loc[1997]).mul(100)
    # combined = pd.concat([data, combined], axis=1)
    # combined.plot(grid=True).get_figure().savefig('view.pdf', format='pdf', dpi=900)

    # # data = combined
    FILE_NAME = 'stat_can_cap.csv'
    # data = read_temporary(FILE_NAME)
    #
    #
    # =========================================================================
    # df = DataFrame(columns=['series_id_1', 'series_id_2', 'r_2'])
    # for pair in combinations(data.columns, 2):
    #     chunk = data.loc[:, list(pair)].dropna(axis=0)
    #     if not chunk.empty:
    #         df = df.append(
    #             {
    #                 'series_id_1': pair[0],
    #                 'series_id_2': pair[1],
    #                 'r_2': r2_score(chunk.iloc[:, 0], chunk.iloc[:, 1])
    #             },
    #             ignore_index=True
    #         )
    # df.to_csv(Path(DIR_EXPORT).joinpath('df.csv'), index=False)
    # =========================================================================

    # # combined = DataFrame()

    # # # =============================================================================
    # # # Manufacturing Indexes
    # # # =============================================================================
    # # # =============================================================================
    # # # {'v11567': 36100386} # Production Indexes
    # # # {'v41707475': 36100309} # Production Indexes
    # # # {'v41708195': 36100309} # Gross Output
    # # # {'v42189127': 36100310} # Production Indexes
    # # # {'v42189751': 36100310} # Gross Output
    # # # {'v64602050': 36100488} # Gross Output
    # # # {'v86718697': 36100217} # Production Indexes
    # # # {'v86719219': 36100217} # Gross Output
    # # # =============================================================================
    # # # SERIES_IDS = {
    # # #     'v86718697': 36100217,
    # # #     'v41707475': 36100309,
    # # #     'v42189127': 36100310,
    # # #     'v11567': 36100386,
    # # # }
    # # # combined = stockpile_can(SERIES_IDS).pipe(mean_series_id)
    # # # combined = combined.div(combined.loc[1961]).mul(100)

    # # # # =============================================================================
    # # # # Gross Output
    # # # # =============================================================================
    # # # SERIES_IDS = {
    # # #     'v86719219': 36100217,
    # # #     'v41708195': 36100309,
    # # #     'v42189751': 36100310,
    # # #     'v64602050': 36100488,
    # # # }
    # # # combined = stockpile_can(SERIES_IDS).pipe(mean_series_id)
    # # # combined = combined.div(combined.loc[1997]).mul(100)
    # # # combined.plot(grid=True).get_figure().savefig(
    # # #     'view.pdf', format='pdf', dpi=900)
    # # # combined.to_csv(Path(DIR_EXPORT).joinpath('df.csv'))

    # =========================================================================
    # FILE_NAME = 'stat_can_cap.csv'
    # data = read_temporary(FILE_NAME)
    # combined = pd.concat(
    #     [data.iloc[:, [_]].dropna(axis=0) for _ in range(30, 35)],
    #     axis=1
    # )
    # combined = combined.div(combined.loc[1997]).mul(100)
    # combined['mean'] = combined.sum(axis=1)
    # combined = combined.iloc[:, [-1]]
    # df = pd.concat([df, combined], axis=1)
    # df.plot(grid=True).get_figure().savefig('view.pdf', format='pdf', dpi=900)
    # =========================================================================

    FILE_NAME = 'stat_can_cap_matching.csv'
    # data = read_temporary(FILE_NAME)
    # data = data[data.iloc[:, 5] != 'Information and communication technologies machinery and equipment']
    # data = data[data.iloc[:, 5] != 'Land']
    # data = data[data.iloc[:, 6] != 'Intellectual property products']
    # # data.dropna(axis=0, how='all').to_csv(Path(DIR).joinpath(FILE_NAME), index=True)

    SERIES_IDS = {
        'v46444624': 36100210,
        'v46444685': 36100210,
        'v46444746': 36100210,
        'v46444990': 36100210,
        'v46445051': 36100210,
        'v46445112': 36100210,
        'v46445356': 36100210,
        'v46445417': 36100210,
        'v46445478': 36100210,
        'v46445722': 36100210,
        'v46445783': 36100210,
        'v46445844': 36100210,
    }
    for series_id in tuple(SERIES_IDS)[::3]:
        stockpile_can({series_id: SERIES_IDS[series_id]}).plot(grid=True)

        # chunk.plot(grid=True).get_figure().savefig(
        #     'temporary.pdf', format='pdf', dpi=900)
    # for series_id in tuple(SERIES_IDS)[1::3]:
    #     stockpile_can({series_id: SERIES_IDS[series_id]}).plot(grid=True)

    # for series_id in tuple(SERIES_IDS)[2::3]:
    #     stockpile_can({series_id: SERIES_IDS[series_id]}).plot(grid=True)


if __name__ == '__main__':
    main()
