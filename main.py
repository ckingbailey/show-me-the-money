""" Oakland PEC Netfile data exploration
"""
import argparse
from time import sleep
import pandas as pd
import requests

BASE_URL = 'https://netfile.com:443/Connect2/api/public'
AID = 'COAK'
HEADERS = { 'Accept': 'application/json' }
PARAMS = { 'aid': AID }

class PageTracker:
    """ Track request pages """
    def __init__(self, start_page=1, last_page=None):
        self._cur_page = start_page
        self._last_page = last_page

    def __lt__(self, value):
        return self._cur_page < value

    def __gt__(self, value):
        return self._cur_page > value

    def __eq__(self, value):
        return self._cur_page == value

    def __le__(self, value):
        return self._cur_page <= value

    def __ge__(self, value):
        return self._cur_page >= value

    @property
    def cur_page(self):
        return self._cur_page

    @property
    def done(self):
        """ Is cur_page the last_page? """
        return self._cur_page == self._last_page

    def incr(self):
        """ Add 1 to current page"""
        self._cur_page += 1

    def print(self):
        """ Print current page without newline """
        end = ' ' if not self.done else '\n'
        print(self._cur_page, end=end, flush=True)


def get_filer_transactions(get_all=False) -> pd.DataFrame:
    """ Get all transactions by filer, returns Pandas DataFrame
    """
    # Collect all filers
    filer_endpoint = f'{BASE_URL}/campaign/list/filer'
    res = requests.get(filer_endpoint, headers=HEADERS, params=PARAMS)
    res.raise_for_status()
    body = res.json()
    print('Filers', end='\n—\n')
    num_pages = body['totalMatchingPages']
    filers = body['filers']
    print(
        f'records returned: {len(filers)}',
        f'total records: {body["totalMatchingCount"]}',
        f'num pages: {num_pages}', sep=' | '
    )
    print('  - Sample filer', filers[0])

    if get_all is True:
        page = PageTracker(last_page=num_pages)
        page.print()
        while page < num_pages:
            page.incr()
            res = requests.get(
                filer_endpoint,
                headers=HEADERS,
                params={ **PARAMS, 'CurrentPageIndex': page.cur_page }
            )
            res.raise_for_status()
            page.print()

            body = res.json()
            filers += body['filers']

            sleep_time = .1 if page.cur_page % 10 == 0 else .25
            sleep(sleep_time)
            
    num_filers = len(filers)
    print('  - Collected total filers', num_filers)

    # Collect transactions for filers
    transactions = []
    filer_transaction_endpoint = f'{BASE_URL}/campaign/export/cal201/transaction/filer'
    for filer in filers[::-1]:
        params = { **PARAMS, 'FilerId': filer['localAgencyId'] }
        res = requests.get(
            filer_transaction_endpoint,
            headers=HEADERS,
            params=params
        )
        res.raise_for_status()

        body = res.json()
        transactions += body['results']
        num_pages = body['totalMatchingPages']
        print('Transactions', end='\n—\n')
        print(
            f'transactions returned {len(transactions)}',
            f'total transactions: {body["totalMatchingCount"]}',
            f'total pages: {num_pages}', sep=' | '
        )
        print('  - Sample transaction', transactions[0])

        if get_all is True:
            page = PageTracker(1)
            page.print()
            while page < num_pages:
                page.incr()
                res = requests.get(
                    filer_transaction_endpoint,
                    params={ **params, 'CurrentPageIndex': page.cur_page},
                    headers=HEADERS
                )
                res.raise_for_status()
                page.print()

                body = res.json()

                transactions += body['results']

                sleep_time = .1 if page.cur_page % 10 == 0 else .25
                sleep(sleep_time)

    print('  - Collected total transactions', len(transactions))
    return pd.DataFrame(transactions)

def get_filings(get_all=False, filter_amended=False):
    """ Collect filings, return Pandas DataFrame
    """
    # Collect Campaign filings
    filing_endpoint = f'{BASE_URL}/list/filing'
    params = { **PARAMS, 'Application': 'Campaign'}
    res = requests.get(
        filing_endpoint,
        headers=HEADERS,
        params=params
    )
    res.raise_for_status()

    body = res.json()
    filings = body['filings']
    num_pages = body['totalMatchingPages']
    print('Filings', end='\n—\n')
    print(f'filings returned: {len(filings)}', f'total filings: {body["totalMatchingCount"]}', f'total pages: {num_pages}', sep=' | ')
    print('  - Sample filing', filings[0])

    if get_all is True:
        page = PageTracker(1)
        page.print()
        while page < num_pages:
            page.incr()
            res = requests.get(
                filing_endpoint,
                headers=HEADERS,
                params={ **params, 'CurrentPageIndex': page.cur_page}
            )
            res.raise_for_status()
            page.print()

            body = res.json()
            filings += body['filings']

            sleep_time = .1 if page.cur_page % 10 == 0 else .25
            sleep(sleep_time)


    print('  - Collected total filings', len(filings))

    # Find a filing with amendmentSequenceNumber (aka report_num)
    if filter_amended is True:
        amendments = [ filing['id'] for filing in filings if filing['amendmentSequenceNumber'] > 0 ]
        print(f'  - Found {len(amendments)} amendment filings')

        amended = [
            filing['amendedFilingId']
            for filing
            in filings if filing['amendedFilingId'] is not None
        ]
        print(f'  - Found {len(amended)} amended filings')
        amended = set(amended)
        print(f'  - {len(set)} after de-dupe')

        filings = [ filing for filing in filings if filing['id'] not in amended ]
        print(f'  - {len(filings)} left after filtering out amended')

    return pd.DataFrame(filings)


def get_transactions_for_filing(filing_id):
    """ Get transactions from filing id
    """
    res = requests.get(
        f'{BASE_URL}/campaign/export/cal201/transaction/filing',
        headers=HEADERS,
        params={ **PARAMS, 'FilingId': filing_id }
    )
    res.raise_for_status()
    body = res.json()

    return body['results']

def main():
    """ Collect all filings
        and see if we can find the report_num key
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint', '-e', required=True, choices=[ 'transactions', 'filings' ])
    parser.add_argument('--save', '-s', action='store_true')
    parser.add_argument('--all', '-a', action='store_true')
    parser.add_argument('--filter_amended', action='store_true')

    args = parser.parse_args()

    programs = {
        'transactions': {
            'function': get_filer_transactions,
            'args': []
        },
        'filings': {
            'function': get_filings,
            'args': [ 'filter_amended' ]
        }
    }
    endpoint = args.endpoint
    print(f'Program: {endpoint}')
    program = programs[endpoint]
    kwargs = {
        'get_all': args.all,
        **{
            k: getattr(args, k) for k in program['args']
        }
    }
    print(f'Arguments: {kwargs}')

    results = program['function'](**kwargs)
    print(f'Got {len(results)} results for endpoint {endpoint}')

    if args.save is True:
        results.to_parquet(args.endpoint, partition_cols=['filerLocalId'])


if __name__ == '__main__':
    main()
