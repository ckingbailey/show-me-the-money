import pandas as pd

def main():
    suzanne = pd.read_csv('itemized_contributions_2022_candidates.csv')
    netfile = pd.read_csv('output/contribs_socrata.csv')

    # T Reid's filer_id
    reid_nf = netfile[
        (netfile['filer_name'] == 'Treva Reid') & (netfile['election_year'] == 2022)
    ][['filer_id', 'tran_id']]

    reid_id = reid_nf['filer_id'].iloc[0]
    reid_pec = suzanne[suzanne['filer_id'] == reid_id]['tran_id']

    unq_reid_trans_nf = set(reid_nf['tran_id'])
    unq_reid_trans_pec = set(reid_pec)

    print(unq_reid_trans_pec - unq_reid_trans_nf)

if __name__ == '__main__':
    main()
