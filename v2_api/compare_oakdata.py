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

    client = sodapy.Socrata(host, None)
    sched_a_fetcher = client.get_all(sched_a_path)
    sched_c_fetcher = client.get_all(sched_c_path)
    sched_e_fetcher = client.get_all(sched_e_path)

    schedule_a = [ result for result in sched_a_fetcher ]
    schedule_c = [ result for result in sched_c_fetcher ]
    schedule_e = [ result for result in sched_e_fetcher ]

    return schedule_a, schedule_c, schedule_e

def main():
    """ Total all schedule A and schedule C transactions
        for committees specified by filer_to_candidate.csv
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true')

    args = parser.parse_args()

    if args.download:
        schedule_a, schedule_c, schedule_e = download_data()
        Path('oakdata_schedule_a.json').write_text(
            json.dumps(schedule_a, indent=4), encoding='utf8')
        Path('oakdata_schedule_c.json').write_text(
            json.dumps(schedule_c, indent=4), encoding='utf8')
        Path('oakdata_schedule_e.json').write_text(
            json.dumps(schedule_e, indent=4), encoding='utf8')
    else:
        schedule_a = json.loads(Path('oakdata_schedule_a.json').read_text(encoding='utf8'))
        schedule_c = json.loads(Path('oakdata_schedule_c.json').read_text(encoding='utf8'))
        schedule_e = json.loads(Path('oakdata_schedule_e.json').read_text(encoding='utf8'))

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
    print(contribs_socrata.groupby(['election_year','filer_name'])['amount'].sum())

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
    print(contribs_oakdata.groupby(['election_year', 'candidate'])[['tran_amt1']].sum())

    expends_socrata = pd.read_csv('output/expends_socrata.csv')
    print(expends_socrata.groupby(['election_year','filer_name'])['amount'].sum())

    schedule_e = committees.merge(
        pd.DataFrame(schedule_e).astype({
            'amount': 'float32'
        }).rename(columns={
            'filer_id': 'sos_id'
        }), how='left', on='sos_id'
    ).rename(columns={
        'sos_id': 'filer_id'
    })
    print(schedule_e.groupby(['election_year','candidate'])[['amount']].sum())


if __name__ == '__main__':
    main()
