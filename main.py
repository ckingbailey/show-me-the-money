""" Oakland PEC Netfile data exploration
"""
from pprint import PrettyPrinter
from time import sleep
import requests

BASE_URL = 'https://netfile.com:443/Connect2/api/public'
AID = 'COAK'

def main():
    """ Collect all filings
        and see if we can find the report_num key
    """
    pp = PrettyPrinter()

    headers = { 'Accept': 'application/json' }
    params = { 'aid': AID }

    # Collect all filers
    res = requests.get(f'{BASE_URL}/campaign/list/filer', headers=headers, params=params)
    body = res.json()
    res.raise_for_status()
    print('records returned', body['totalMatchingCount'], 'num pages', body['totalMatchingPages'])
    print('  sample filer', body['filers'][0])
    filers = [ *body['filers'] ]

    # Collect all transactions for filers
    num_results = 0
    records_queried = 0
    num_filers = body['totalMatchingCount']
    print('num filers', num_filers)

    # Find a filer that has a transaction
    for filer in filers[::-1]:
        res = requests.get(
            f'{BASE_URL}/campaign/export/cal201/transaction/filer',
            headers=headers,
            params={ **params, 'FilerId': filer['localAgencyId'] }
        )
        res.raise_for_status()

        records_queried += 1
        print(records_queried, end=' ', flush=True)

        body = res.json()
        num_results = body['totalMatchingCount']
        if num_results:
            print(
                'records returned',
                body['totalMatchingCount'],
                'num pages',
                body['totalMatchingPages']
            )
            print('  sample filing:')
            print(body['results'][0])
            transactions = [ *body['results'] ]
            break

        sleep(.5)

    # Collect all Campaign filings
    res = requests.get(
        f'{BASE_URL}/list/filing',
        headers=headers,
        params={ **params, 'Application': 'Campaign'}
    )
    res.raise_for_status()

    body = res.json()
    filings = [ *body['filings'] ]
    print(
        'num records returned from filings', body['totalMatchingCount'],
        'num pages', body['totalMatchingPages']
    )
    print('  sample filing', filings[0])

    # Find a filing with amendmentSequenceNumber (aka report_num)
    amended = [ filing['id'] for filing in filings if filing['amendmentSequenceNumber'] > 0 ]
    print(f'    Found {len(amended)} amended filings')

    # Get transactions from amended filings
    amended_transactions = []
    for a in amended:
        res = requests.get(
            f'{BASE_URL}/campaign/export/cal201/transaction/filing',
            headers=headers,
            params={ **params, 'FilingId': a }
        )
        res.raise_for_status()
        body = res.json()

        amended_transactions += body['results']

    print('Sample response for transaction by filingId')
    pp.pprint(amended_transactions[-1])

    related_filing = [ f for f in filings if f['id'] == amended_transactions[-1]['filingId'] ]
    print('  and the related filing', related_filing)

if __name__ == '__main__':
    main()
