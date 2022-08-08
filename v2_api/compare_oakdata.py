import argparse
import json
from pathlib import Path
import pandas as pd
import sodapy

def get_tran_amts(trans: list[dict], committees: list[dict], election_year: int=None):
    """ Total tran_amt1 and tran_amt2
        for all committees that appear in provided list
    """
    if election_year is not None:
        year_committees = [
            c['sos_id'] for c in committees if c['election_year'] == election_year
        ]
    else:
        year_committees = [ c['sos_id'] for c in committees ]

    return [
        float(t['tran_amt1']) + float(t['tran_amt2'])
        for t in trans
        if str(t.get('filer_id')) in year_committees
    ]

def download_data():
    """ Download all schedule a and schedule c from Socrata
    """
    host = 'data.oaklandca.gov'
    sched_a_path = '3xq4-ermg'
    sched_c_path = 'ba44-jqtm'
    sched_e_path = 'bvfu-nq99'
    summary_path = 'rsxe-vvuw'

    client = sodapy.Socrata(host, None)
    sched_a_fetcher = client.get_all(sched_a_path)
    sched_c_fetcher = client.get_all(sched_c_path)
    sched_e_fetcher = client.get_all(sched_e_path)
    summary_fetcher = client.get_all(summary_path)

    schedule_a = [ result for result in sched_a_fetcher ]
    schedule_c = [ result for result in sched_c_fetcher ]
    schedule_e = [ result for result in sched_e_fetcher ]
    summary = [ result for result in summary_fetcher ]

    return schedule_a, schedule_c, schedule_e, summary

def main():
    """ Total all schedule A and schedule C transactions
        for committees specified by filer_to_candidate.csv
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true')

    args = parser.parse_args()

    if args.download:
        schedule_a, schedule_c, schedule_e, summary = download_data()
        Path('oakdata_schedule_a.json').write_text(
            json.dumps(schedule_a, indent=4), encoding='utf8')
        Path('oakdata_schedule_c.json').write_text(
            json.dumps(schedule_c, indent=4), encoding='utf8')
        Path('oakdata_schedule_e.json').write_text(
            json.dumps(schedule_e, indent=4), encoding='utf8')
        Path('oakdata_460_summary.json').write_text(
            json.dumps(summary, indent=4), encoding='utf8'
        )
    else:
        schedule_a = json.loads(Path('oakdata_schedule_a.json').read_text(encoding='utf8'))
        schedule_c = json.loads(Path('oakdata_schedule_c.json').read_text(encoding='utf8'))
        schedule_e = json.loads(Path('oakdata_schedule_e.json').read_text(encoding='utf8'))
        summary = json.loads(Path('oakdata_460_summary.json').read_text(encoding='utf8'))

    committees = pd.read_csv('filer_to_candidate.csv')[
        ['Local Agency ID', 'SOS ID', 'election_year', 'candidate']
    ].rename(columns={
        'Local Agency ID': 'local_agency_id',
        'SOS ID': 'sos_id'
    }, inplace=False).astype({
        'local_agency_id': 'string',
        'sos_id': 'string'
    })

    # filer_id_years = committees[['sos_id', 'election_year']].to_dict(orient='records')
    # a_2020_trans = get_tran_amts(schedule_a, filer_id_years, 2020)

    contribs_socrata = pd.read_csv('output/contribs_socrata.csv')
    # print(contribs_socrata.groupby(['election_year','filer_name'])['amount'].sum())

    schedule_a = committees.merge(
        pd.DataFrame(schedule_a).astype({
            'tran_amt1': 'float32',
            'tran_amt2': 'float32'
        }).rename(columns={
            'filer_id': 'sos_id'
        }), how='left', on='sos_id'
    ).rename(columns={
        'sos_id': 'filer_id'
    })
    schedule_c = committees.merge(
        pd.DataFrame(schedule_c).astype({
            'tran_amt1': 'float32',
            'tran_amt2': 'float32'
        }).rename(columns={
            'filer_id': 'sos_id'
        }), how='left', on='sos_id'
    ).rename(columns={
        'sos_id': 'filer_id'
    })
    contribs_oakdata = pd.concat([schedule_a, schedule_c])
    contribs_oakdata['candidate'] = contribs_oakdata['candidate'].apply(lambda c: c.strip())
    contribs_oakdata_totals = contribs_oakdata.astype({
        'tran_amt1': float
    }).round(0).groupby(
        ['election_year', 'candidate']
    ).agg({
        'tran_amt1': 'sum',
        'tran_id': 'count'
    }).rename(columns={
        'tran_amt1': 'amt_oakdata',
        'tran_id': 'ct_oakd'
    })
    contribs_socrata_totals = contribs_socrata.rename(columns={
        'filer_name': 'candidate'
    }).groupby(
        ['election_year','candidate']
    ).agg({
        'amount': 'sum',
        'tran_id': 'count'
    }).rename(columns={
        'amount': 'amt_netfile',
        'tran_id': 'ct_netf'
    }).astype({
        'amt_netfile': float
    }).round(0)

    contribs_excel = pd.read_csv('contribs_excel.csv')
    contribs_excel['candidate'] = contribs_excel['candidate'].apply(lambda c: c.strip())

    contribs_excel_totals = contribs_excel.groupby(
        ['election_year', 'candidate']
    ).agg({
        'tran_amt1': 'sum',
        'tran_ct': 'sum'
    }).rename(columns={
        'tran_amt1': 'amt_excel',
        'tran_ct': 'ct_excl'
    })

    all_contribs = contribs_oakdata_totals.merge(contribs_socrata_totals,
        how='inner',
        on=['election_year', 'candidate']).merge(contribs_excel_totals,
        how='inner',
        on=['election_year', 'candidate'])
    all_contribs['eq_n_o'] = all_contribs['amt_netfile'] - all_contribs['amt_oakdata']
    all_contribs['eq_n_e'] = all_contribs['amt_netfile'] - all_contribs['amt_excel']
    all_contribs['eq_n_o'] = all_contribs['eq_n_o'].apply(lambda x: '✔︎' if x == 0 else x)
    all_contribs['eq_n_e'] = all_contribs['eq_n_e'].apply(lambda x: '✔︎' if x == 0 else x)

    print(all_contribs[[
        'amt_netfile', 'ct_netf',
        'amt_oakdata', 'ct_oakd',
        'eq_n_o',
        'amt_excel', 'ct_excl',
        'eq_n_e'
    ]])

    expends_socrata = pd.read_csv('output/expends_socrata.csv')

    schedule_e = committees.merge(
        pd.DataFrame(schedule_e).astype({
            'amount': float
        }).rename(columns={
            'filer_id': 'sos_id',
        }), how='left', on='sos_id'
    ).rename(columns={
        'sos_id': 'filer_id'
    })
    expends_oakdata_totals = schedule_e.groupby(
        ['election_year','candidate']
    ).agg({
        'amount': 'sum',
        'tran_id': 'count'
    }).rename(columns={
        'amount': 'amt_oakdata',
        'tran_id': 'ct_oakd'
    }).round(0)

    expends_netfile_totals = expends_socrata.rename(columns={
        'filer_name': 'candidate'
    }).groupby(
        ['election_year','candidate']
    ).agg({
        'amount': 'sum',
        'tran_id': 'count'
    }).rename(columns={
        'amount': 'amt_netfile',
        'tran_id': 'ct_netf'
    })

    expends_excel = pd.read_csv('expends_excel.csv')
    expends_excel_totals = expends_excel.groupby(
        ['election_year','candidate']
    ).agg({
        'tran_amt1': 'sum',
        'tran_ct': 'sum'
    }).rename(columns={
        'tran_amt1': 'amt_excel',
        'tran_ct': 'ct_excl'
    })

    all_expends = expends_oakdata_totals.merge(expends_netfile_totals,
        on=['election_year','candidate'],
        how='inner').merge(expends_excel_totals,
        on=['election_year','candidate'],
        how='inner'
        ).round(0)

    all_expends['eq_n_o'] = all_expends['amt_netfile'] - all_expends['amt_oakdata']
    all_expends['eq_n_o'] = all_expends['eq_n_o'].apply(lambda x: '✔︎' if x == 0 else x)
    all_expends['eq_n_e'] = all_expends['amt_netfile'] - all_expends['amt_excel']
    all_expends['eq_n_e'] = all_expends['eq_n_e'].apply(lambda x: '✔︎' if x == 0 else x)

    print(all_expends[[
        'amt_netfile', 'ct_netf',
        'amt_oakdata', 'ct_oakd',
        'eq_n_o',
        'amt_excel', 'ct_excl',
        'eq_n_e'
    ]])

if __name__ == '__main__':
    main()
