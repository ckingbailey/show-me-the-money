from datetime import datetime
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
            'elementNid': t['elementNid'],
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

    unfiltered_trans = json.loads(
        Path('example/unfiltered_transactions.json').read_text(encoding='utf8'))
    print(len(unfiltered_trans))
    unfiltered_trans = pd.DataFrame([
        {
            'elementNid': t['elementNid'],
            'filer_nid': t['filerNid'],
            'tranId': t['transaction']['tranId'] if t.get('transaction') is not None else t['transactionId'],
            'filing_nid': t['filingNid'],
            'tranAmt1': t['transaction']['tranAmt1'] if t.get('transaction') else None,
            'tranDate': t['transaction']['tranDate'] if t.get('transaction') else None,
            'tranNamL': t['transaction']['tranNamL'] if t.get('transaction') else None
        } for t in unfiltered_trans
    ])

    rectified_trans = unfiltered_trans.merge(
        trans.drop(columns=[
            'tranNamL','tranDate','tranAmt1','filer_nid'
        ]).rename(columns={
            'tranId': 'old_tran_id'
        }), on=['elementNid'], how='left'
    )
    rectified_trans['tranId'] = rectified_trans['tranId'].fillna(rectified_trans['old_tran_id'])

    print('Did I find missing tranIds from all trans despite issues fetching them?',
        rectified_trans[rectified_trans['tranId'].isin(missing_trans)][[
            'elementNid','filer_nid','tranId','tranAmt1','tranDate','tranNamL'
        ]],
        '====================', sep='\n')

    print('All Reid trans from around the time of the missing ones',
        trans[(trans['filer_nid'] == reid_nid)
        & (pd.to_datetime(trans['tranDate']) > datetime(2021, 12, 29))
        & (pd.to_datetime(trans['tranDate']) < datetime(2021, 12, 31))
        & (trans['tranId'].str.startswith('INC'))].sort_values('tranNamL'),
        '====================', sep='\n')

    filings = json.loads(Path('example/filings.json').read_text(encoding='utf8'))
    filings = pd.DataFrame([
        {
            'filer_nid': f['filerMeta']['filerId'],
            'filing_nid': f['filingNid'],
            'form': f['specificationRef']['name'],
            'filing_start_date': f['filingMeta']['startDate'],
            'filing_end_date': f['filingMeta']['endDate']
        } for f in filings
    ])
    reid2022_filings = filings[
        (filings['filer_nid'] == reid_nid)
        & (filings['form'].isin(['FPPC460','FPPC497']))]
    print('Reid Mayor 2022 finance filings', reid2022_filings.drop(
        columns=['filing_start_date','filer_nid']
        ).sort_values('filing_end_date'),
        '====================', sep='\n')

    filing_of_interest = reid2022_filings[reid2022_filings['filing_end_date'] == '2021-12-31']
    print(f'filing_nid where the missing trans should be found {filing_of_interest["filing_nid"].iloc[0]}')

    trans_for_filing = filing_of_interest.merge(
        rectified_trans,
        on='filing_nid',
        how='left'
    )
    # default_display_rows = pd.get_option('display.max_rows')
    # pd.set_option('display.max_rows', None)
    print(trans_for_filing[['tranId','tranNamL','tranAmt1','tranDate']].sort_values('tranNamL'))

    # pd.set_option('display.max_rows', default_display_rows)

    trans_for_names = rectified_trans[rectified_trans['tranNamL'].isin([
        'Tischler','Taplin','Jacobson'])]

    default_display_rows = pd.get_option('display.max_rows')
    pd.set_option('display.max_rows', None)
    print(trans_for_names.merge(filers,
        on='filer_nid',
        how='inner')[['filer_nid','filer_name','tranId','tranNamL','tranAmt1','tranDate']])
    pd.set_option('display.max_rows', default_display_rows)

if __name__ == '__main__':
    main()
