import argparse
from ast import arg
import json
from pprint import PrettyPrinter
from .create_socrata_csv import AUTH, PARAMS, BASE_URL, session, save_source_data
from .late_contributions import load_filings, load_from_file

filing_elements = 'filing_elements'

def get_filing_elements(filing_nid):
    """ Get filing-elements by filing_nid """
    path = '/filing/v101/filing-elements'
    params = {
        **PARAMS,
        'filingNid': filing_nid
    }

    res = session.get(f'{BASE_URL}/{path}', params=params, auth=AUTH)
    f_elements = res.json()
    return f_elements['results']

def load_filing_elements():
    """ Load filing elements from disk """
    return load_from_file(filing_elements)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true')

    args = parser.parse_args()

    pp = PrettyPrinter()
    filings = load_filings()
    four_sixties = [ f for f in filings if f['specificationRef']['name'] == 'FPPC460' ]

    f_elements = (get_filing_elements(four_sixties[0]['filingNid'])
        if args.download
        else load_filing_elements())
    save_source_data({ filing_elements: f_elements })

    pp.pprint(f_elements[0])

if __name__ == '__main__':
    main()
