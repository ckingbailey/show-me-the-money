import json
from .create_socrata_csv import AUTH, PARAMS, BASE_URL, session
from .late_contributions import load_filings

def get_filing_elements(filing_nid):
    """ Get filing-elements by filing_nid """
    path = '/filing/v101/filing-elements'
    params = {
        **PARAMS,
        'filingNid': filing_nid
    }

    res = session.get(f'{BASE_URL}/{path}', params=params, auth=AUTH)
    f_elements = res.json()
    print(f_elements['results'])

def main():
    filings = load_filings()
    get_filing_elements(filings[0]['filingNid'])

if __name__ == '__main__':
    main()
