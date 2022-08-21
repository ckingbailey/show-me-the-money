import argparse
from datetime import datetime
import json
from pathlib import Path
import pandas as pd

def load_excel_contribs_itemized():
    """ Get DataFrame of itemized transactions from Oakdata """
    return pd.read_csv('itemized_contributions_2022_candidates.csv')

def load_show_me_the_money_contribs():
    """ Get DataFrame of saved contributions for Show Me The Money app """
    return pd.read_csv('output/contribs_socrata.csv')

def load_show_me_the_money_expends():
    """ Get DataFrame of saved expenditures for Show Me The Money app
        source from new NetFile API
    """
    return pd.read_csv('output/expends_socrata.csv')

def load_filers():
    """ Get DataFrame of filers sourced from new NetFile API """
    filers = json.loads(Path('example/filers.json').read_text(encoding='utf8'))
    return pd.DataFrame([
        {
            'filer_nid': f['filerNid'],
                        'filer_id': f['registrations'].get('CA SOS'),
            'filer_name': f['filerName'],
            'candidate_name': f['candidateName']
        } for f in filers
    ]).astype({
        'filer_id': 'string'
    })

def load_transactions():
    """ Get DataFrame of transactions sourced from new NetFile API
        by filer ID
    """
    trans = json.loads(Path('example/transactions.json').read_text(encoding='utf8'))
    return pd.DataFrame([
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

def load_unfiltered_trans():
    """ Get DataFrame of transactions sourced from NetFile API
        without dependency on filer
    """
    unfiltered_trans = json.loads(
        Path('example/unfiltered_transactions.json').read_text(encoding='utf8'))
    print(len(unfiltered_trans))
    return pd.DataFrame([
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

def load_filings():
    """ Get DataFrame of filings sourced from NetFile API """
    filings = json.loads(Path('example/filings.json').read_text(encoding='utf8'))
    return pd.DataFrame([
        {
            'filer_nid': f['filerMeta']['filerId'],
            'filing_nid': f['filingNid'],
            'form': f['specificationRef']['name'],
            'filing_start_date': f['filingMeta']['startDate'],
            'filing_end_date': f['filingMeta']['endDate']
        } for f in filings
    ])

def get_missing_tran_ids(contribs_a: pd.DataFrame, contribs_b: pd.DataFrame) -> set[str]:
    unq_cand_trans_nf = set(contribs_a['tran_id'])
    unq_cand_trans_pec = set(contribs_b['tran_id'])

    missing_trans = unq_cand_trans_pec - unq_cand_trans_nf

    print('> IDs of missing transactions', missing_trans, end='\n\n')

    return missing_trans

def show_trans_from_filing(
    filings: pd.DataFrame, trans: pd.DataFrame,
    candidate_nid: int, candidate_name: str) -> None:
    """ Show transactions from filing that should contain missing transactions """
    candidate2022_filings = filings[
        (filings['filer_nid'] == candidate_nid)
        & (filings['form'].isin(['FPPC460','FPPC497']))]
    print(f'{candidate_name} 2022 finance filings', candidate2022_filings.drop(
        columns=['filing_start_date','filer_nid']
        ).sort_values('filing_end_date'),
        '====================', sep='\n')

    filing_date = '2021-12-31'
    filing_of_interest = candidate2022_filings[candidate2022_filings['filing_end_date'] == filing_date]
    print(f'filing_nid where the missing trans should be found {filing_of_interest["filing_nid"].iloc[0]}')

    trans_for_filing = filing_of_interest.merge(
        trans,
        on='filing_nid',
        how='left'
    )
    print(trans_for_filing[['tranId','tranNamL','tranAmt1','tranDate']].sort_values('tranNamL'))

def show_trans_for_contributors(
    trans: pd.DataFrame,
    filers: pd.DataFrame,
    contributors: list[str]) -> None:
    """ Show all transactions for list of contributors' last names """
    trans_for_names = trans[trans['tranNamL'].isin(contributors)]

    default_display_rows = pd.get_option('display.max_rows')
    pd.set_option('display.max_rows', None)
    print(trans_for_names.merge(filers,
        on='filer_nid',
        how='inner')[['filer_nid','filer_name','tranId','tranNamL','tranAmt1','tranDate']])
    pd.set_option('display.max_rows', default_display_rows)

def find_trans_for_filer(trans, filer_name, election_year):
    """ Return DataFrame of transactions where filer_name and election_year match inputs """
    return trans.loc[(trans['filer_name'] == filer_name) & (trans['election_year'] == election_year)]

def  find_cand_id_from_trans(trans):
    return trans['filer_id'].iloc[0]

def search_for_missing_contribs(
    cand_name,
    election_year,
    filings,
    trans,
    filers) -> None:
    """ Show data around missing tran_ids """
    contribs_pec = load_excel_contribs_itemized()
    all_trans = pd.read_csv('example/all_trans.csv')

    contribs_smtm = load_show_me_the_money_contribs()
    cand_contribs_smtm = find_trans_for_filer(contribs_smtm, cand_name, election_year)

    cand_id = find_cand_id_from_trans(cand_contribs_smtm)

    cand_contribs_pec = contribs_pec[contribs_pec['filer_id'] == cand_id]
    missing_trans = get_missing_tran_ids(contribs_smtm, cand_contribs_pec)
    print(f'> {cand_name} filer id:', cand_id)

    cand_nid = filers[filers['filer_id'] == str(cand_id)]['filer_nid'].iloc[0]
    print(f'> {cand_name} filer_nid', cand_nid, end='\n\n')

    print(f'{cand_name} transactions matching missing tran_ids in Suzanne\'s data set', cand_contribs_pec[
        cand_contribs_pec['tran_id'].isin(missing_trans)
    ][
        ['tran_id_unique', 'tran_id', 'tran_amt1', 'tran_date', 'tran_naml', 'report_num', 'rpt_date']
    ].sort_values(
        by=['tran_date', 'tran_id']
    ), '====================', sep='\n')

    print(f'Same-ID\'d transactions for {cand_name} from all_trans.csv', all_trans[
        (all_trans['tran_id'].isin(missing_trans))
        & (all_trans['filer_name'].str.contains('Reid'))
    ][['filer_id', 'filer_name', 'filing_id', 'tran_id', 'amount', 'receipt_date', 'filing_date', 'contributor_name']].sort_values(
        by=['filing_date', 'tran_id']
    ), '====================', sep='\n')

    print(
        'Matching tranIds from raw transactions JSON',
        trans[trans['tranId'].isin(missing_trans)].merge(
            filers[['filer_nid','filer_id','filer_name']],
            how='inner',
            on='filer_nid'
        ), '====================', sep='\n')

    candidate_last_name = cand_name.split(' ')[-1]
    print(
        f'Matching tranIds where name is "{candidate_last_name}" from raw transactions JSON',
        trans[trans['tranId'].isin(missing_trans)].merge(
            filers[filers['filer_name'].str.contains(candidate_last_name)][['filer_nid','filer_id','filer_name']],
            how='inner',
            on='filer_nid'
        ), '====================', sep='\n')

    print('Did I find missing tranIds from all trans despite issues fetching them?',
        trans[trans['tranId'].isin(missing_trans)][[
            'elementNid','filer_nid','tranId','tranAmt1','tranDate','tranNamL'
        ]],
        '====================', sep='\n')

    print(f'All {cand_name} trans from around the time of the missing ones',
        trans[(trans['filer_nid'] == cand_nid)
        & (pd.to_datetime(trans['tranDate']) > datetime(2021, 12, 29))
        & (pd.to_datetime(trans['tranDate']) < datetime(2021, 12, 31))
        & (trans['tranId'].str.startswith('INC'))].sort_values('tranNamL'),
        '====================', sep='\n')

    show_trans_from_filing(filings, trans, cand_nid, cand_name)

    show_trans_for_contributors(trans, filers, ['Tischler','Taplin','Jacobson'])

def show_expends_by_year(
    cand_name,
    election_year,
    filings,
    trans,
    filers) -> None:
    """ Count tran_ids by election year """
    expends_smtm = load_show_me_the_money_expends()
    expends_smtm['expenditure_year'] = pd.to_datetime(expends_smtm['expenditure_date']).dt.year
    cand_expends_smtm = find_trans_for_filer(expends_smtm, cand_name, election_year)

    cand_id = find_cand_id_from_trans(cand_expends_smtm)

    exp_by_year = cand_expends_smtm.groupby(
        'expenditure_year'
    ).agg({
        'tran_id': 'count'
    })
    print('Count of expenditures by year from NetFile API',
        exp_by_year.head(),
        '====================', sep='\n')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('program', choices=['contributions', 'expenditures'])
    parser.add_argument('--candidate', '-c', required=True, help='Name of candidate to search for')
    parser.add_argument('--election_year', '-y', required=True, help='Election year to search in')

    args = parser.parse_args()

    candidate_name = args.candidate
    election_year = int(args.election_year or 0) or None
    program = {
        'contributions': search_for_missing_contribs,
        'expenditures': show_expends_by_year
    }[args.program]

    trans = load_transactions()
    unfiltered_trans = load_unfiltered_trans()
    rectified_trans = unfiltered_trans.merge(
        trans.drop(columns=[
            'tranNamL','tranDate','tranAmt1','filer_nid'
        ]).rename(columns={
            'tranId': 'old_tran_id'
        }), on=['elementNid'], how='left'
    )
    rectified_trans['tranId'] = rectified_trans['tranId'].fillna(rectified_trans['old_tran_id'])

    filings = load_filings()

    filers = load_filers()
    print(
        f'Filers matching {candidate_name}',
        filers[filers['filer_name'].str.contains(candidate_name)],
        '====================', sep='\n')

    program(candidate_name, election_year, filings, rectified_trans, filers)

if __name__ == '__main__':
    main()
