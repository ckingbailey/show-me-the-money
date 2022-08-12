import json
from pathlib import Path
import pandas as pd

def main():
    suzanne = pd.read_csv('itemized_contributions_2022_candidates.csv')
    netfile = pd.read_csv('output/contribs_socrata.csv')

    # T Reid's filer_id
    reid_nf = netfile[
        (netfile['filer_name'] == 'Treva Reid') & (netfile['election_year'] == 2022)
    ]

    reid_id = reid_nf['filer_id'].iloc[0]
    reid_pec = suzanne[suzanne['filer_id'] == reid_id]

    unq_reid_trans_nf = set(reid_nf['tran_id'])
    unq_reid_trans_pec = set(reid_pec['tran_id'])

    missing_trans = unq_reid_trans_pec - unq_reid_trans_nf

    print(reid_id)
    print(missing_trans)

    print(reid_pec[
        reid_pec['tran_id'].isin(missing_trans)
    ][
        ['tran_id_unique', 'tran_id', 'tran_amt1', 'tran_date', 'tran_naml', 'report_num', 'rpt_date']
    ].sort_values(
        by=['tran_date', 'tran_id']
    ))

    all_trans = pd.read_csv('example/all_trans.csv')
    print(all_trans[
        (all_trans['tran_id'].isin(missing_trans))
        & (all_trans['filer_name'].str.contains('Reid'))
    ][['filer_id', 'filer_name', 'filing_id', 'tran_id', 'amount', 'receipt_date', 'filing_date', 'contributor_name']].sort_values(
        by=['filing_date', 'tran_id']
    ))

    filers = json.loads(Path('example/filers.json').read_text(encoding='utf8'))
    filers = pd.DataFrame([
        {
            'filerNid': f['filerNid'],
            'filer_id': f['registrations'].get('CA SOS'),
            'filerName': f['filerName'],
            'candidateName': f['candidateName']
        } for f in filers
    ])
    filers = filers[(filers['filer_id'].notna()) & (filers['filer_id'] != 'Pending')][['filerNid', 'filer_id']]
    print(len(filers.index))
    filers = filers[filers['filer_id'] == reid_id]

    trans = json.loads(Path('example/transactions.json').read_text(encoding='utf8'))
    trans = pd.DataFrame([
        {
            'filerNid': t['filerNid'],
            'tranId': t['transaction']['tranId'],
            'filingNid': t['filingNid'],
            'transactionId': t['transactionId'],
            'tranAmt1': t['transaction']['tranAmt1'],
            'tranDate': t['transaction']['tranDate'],
            'calculatedDate': t['calculatedDate'],
            'tranNamL': t['transaction']['tranNamL'],
            'isItemized': t['isItemized']
        } for t in trans
    ])
    print(trans[trans['tranId'].isin(missing_trans)])

if __name__ == '__main__':
    main()
