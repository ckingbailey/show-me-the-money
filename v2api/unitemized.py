import argparse
from ast import arg
import json
from pathlib import Path
from pprint import PrettyPrinter
from .create_socrata_csv import AUTH, PARAMS, BASE_URL, session, save_source_data

filing_elements = 'filing_elements'

def get_filing_elements_by_filing(filing_nid):
    """ Get filing-elements by filing_nid """
    path = '/filing/v101/filing-elements'
    params = {
        **PARAMS,
        'filingNid': filing_nid
    }

    res = session.get(f'{BASE_URL}/{path}', params=params, auth=AUTH)
    f_elements = res.json()
    return f_elements['results']

def get_multiple_filing_elements(filings):
    filing_elements = []
    for f in filings:
        filing_nid = f['filingNid']
        filing_elements += get_filing_elements_by_filing(filing_nid)

    return filing_elements

def load_from_file(filepath:str) -> list[dict]:
    """ Load json file from file name, without extension, within example/ folder """
    return json.loads(Path(f'example/{filepath}.json').read_text(encoding='utf8'))

def load_filings() -> list[dict]:
    """ Load filings from disk """
    return load_from_file('filings')

def load_filers() -> list[dict]:
    """ Load filers from disk """
    return load_from_file('filers')

def load_filing_elements():
    """ Load filing elements from disk """
    return load_from_file(filing_elements)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true')

    args = parser.parse_args()

    pp = PrettyPrinter()
    filings = load_filings()

    filers = load_filers()

    t_reid = [ f for f in filers if 'Reid' in f['filerName'] and '2022' in f['filerName'] ][0]
    reid_nid = t_reid['filerNid']

    reid_filings = [ f for f in filings if int(f['filerMeta']['filerId']) == int(reid_nid) ]

    f_elements = (get_multiple_filing_elements(reid_filings)
        if args.download
        else load_filing_elements())
    save_source_data({ filing_elements: f_elements })

    filing_parts = {}
    filing_dates = {}
    for f in f_elements:
        filing_nid = f['filingNid']

        if filing_nid not in filing_dates:
            filing = [ f for f in filings if f['filingNid'] == filing_nid ][0]
            filing_date = filing['filingMeta']['legalFilingDate']
            filing_dates[filing_nid] = filing_date
        else:
            filing_date = filing_dates[filing_nid]

        if filing_date not in filing_parts:
            filing_parts[filing_date] = []

        filing_parts[filing_date].append(f)

    pp.pprint({
        d: f['elementModel']['scheduleA']['line2']
        for d, parts
        in filing_parts.items()
            for f in parts
            if f['elementActivityType'] != 'Superseded'
            and f['elementClassification'] == 'Summary'
    })

if __name__ == '__main__':
    main()
