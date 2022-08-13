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

    print('> Reid filer id:', reid_id)
    print('> IDs of missing transactions', missing_trans, end='\n\n')

    print('Reid transactions in Suzanne\'s data set', reid_pec[
        reid_pec['tran_id'].isin(missing_trans)
    ][
        ['tran_id_unique', 'tran_id', 'tran_amt1', 'tran_date', 'tran_naml', 'report_num', 'rpt_date']
    ].sort_values(
        by=['tran_date', 'tran_id']
    ), '====================', sep='\n')

    all_trans = pd.read_csv('example/all_trans.csv')
    print('Same-ID\'d transactions for Reid from all_trans.csv', all_trans[
        (all_trans['tran_id'].isin(missing_trans))
        & (all_trans['filer_name'].str.contains('Reid'))
    ][['filer_id', 'filer_name', 'filing_id', 'tran_id', 'amount', 'receipt_date', 'filing_date', 'contributor_name']].sort_values(
        by=['filing_date', 'tran_id']
    ), '====================', sep='\n')

    filers = json.loads(Path('example/filers.json').read_text(encoding='utf8'))
    filers = pd.DataFrame([
        {
            'filer_nid': f['filerNid'],
            'filer_id': f['registrations'].get('CA SOS'),
            'filer_name': f['filerName'],
            'candidate_name': f['candidateName']
        } for f in filers
    ]).astype({
        'filer_id': 'string'
    })
    print(
        'Filers matching Reid',
        filers[filers['filer_id'] == str(reid_id)],
        '====================', sep='\n')
    reid_nid = filers[filers['filer_id'] == str(reid_id)]['filer_nid'].iloc[0]
    print('> Reid filer_nid', reid_nid, end='\n\n')

    trans = json.loads(Path('example/transactions.json').read_text(encoding='utf8'))
    trans = pd.DataFrame([
        {
            'filer_nid': t['filerNid'],
            'tranId': t['transaction']['tranId'],
            'filingNid': t['filingNid'],
            'tranAmt1': t['transaction']['tranAmt1'],
            'tranDate': t['transaction']['tranDate'],
            'tranNamL': t['transaction']['tranNamL'],
        } for t in trans
    ])
    print(
        'Matching tranIds from raw transactions JSON',
        trans[trans['tranId'].isin(missing_trans)].merge(
            filers[['filer_nid','filer_id','filer_name']],
            how='inner',
            on='filer_nid'
        ), '====================', sep='\n')

    print(
        'Matching tranIds where name is "Reid" from raw transactions JSON',
        trans[trans['tranId'].isin(missing_trans)].merge(
            filers[filers['filer_name'].str.contains('Reid')][['filer_nid','filer_id','filer_name']],
            how='inner',
            on='filer_nid'
        ), '====================', sep='\n')

    print(
        'So then who did receive these transactions?',
        trans[trans['tranId'].isin(missing_trans)].merge(
            filers[['filer_nid','filer_id','filer_name']],
            how='inner',
            on='filer_nid'
        ).groupby(['filer_name','filer_nid','filer_id']).agg({
            'tranId': 'count',
            'tranNamL': 'count'
        }), '====================', sep='\n')

if __name__ == '__main__':
    main()
