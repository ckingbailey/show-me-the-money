""" Oakland PEC Netfile data exploration
"""
import argparse
from pathlib import Path
import re
from time import sleep
from xmlrpc.client import DateTime
import pandas as pd
import requests
from sqlalchemy import create_engine, types as sq_types # pylint: disable=import-error

BASE_URL = 'https://netfile.com:443/Connect2/api/public'
AID = 'COAK'
HEADERS = { 'Accept': 'application/json' }
PARAMS = { 'aid': AID }

class PageTracker:
    """ Track request pages """
    def __init__(self, start_page=0, last_page=None):
        self._cur_page = start_page
        self._last_page = last_page or start_page

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
    def last_page(self):
        return self._last_page

    @property
    def done(self):
        """ Is cur_page the last_page? """
        return self._cur_page >= self._last_page

    def incr(self):
        """ Add 1 to current page"""
        self._cur_page += 1

    def print(self):
        """ Print current page without newline """
        end = ' ' if self.done is False else '\n'
        print(self._cur_page, end=end, flush=True)

class NetfileClient:
    """ Query Netfile """
    page = PageTracker()
    records_key = 'results'

    def __init__(self):
        self.base_url = 'https://netfile.com:443/Connect2/api/public'
        self.headers = { 'Accept': 'application/json' }
        self.params = { 'aid': 'COAK' }

        # endpoints
        self.filings = FilingsClient()
        self.transactions = TransactionsClient()


class BaseEndpointClient:
    """ provide generic fetch function for Netfile endpoints """
    def __init__(self):
        self.base_url = 'https://netfile.com:443/Connect2/api/public'
        self._path = ''
        self._url = f'{self.base_url}{self._path}'
        self.headers = { 'Accept': 'application/json' }
        self.params = { 'aid': 'COAK' }
        self.records_key = 'results'

    @property
    def path(self):
        """ path getter """
        return self._path

    @path.setter
    def path(self, p):
        self._path = p
        self._url = f'{self.base_url}{self.path}'

    @property
    def url(self):
        """ url getter """
        return self._url

    def fetch(self, pages=1):
        """ fetch one record or many """
        res = requests.get(
            self.url,
            headers=self.headers,
            params=self.params
        )
        res.raise_for_status()

        body = res.json()
        last_page = body['totalMatchingPages']
        print(f'Found {last_page} total matching pages')
        records_key = self.records_key
        records = body[records_key]

        url = res.url

        page = (PageTracker(start_page=1, last_page=pages)
            if pages > 0
            else PageTracker(start_page=1, last_page=last_page))
        page.print()
        page.incr()

        while page.done is False:
            params = {
                **self.params,
                'CurrentPageIndex': page.cur_page
            }
            res = requests.get(
                url,
                headers=self.headers,
                params=params
            )
            res.raise_for_status()
            body = res.json()

            records += body[records_key]
            # page.print()
            page.print()
            page.incr()

        return records

class FilingsClient(BaseEndpointClient):
    """ Fetch filings """
    def __init__(self):
        super().__init__()
        self.path = '/list/filing'
        self.records_key = 'filings'
        self.params = {
            **self.params,
            'Application': 'Campaign'
        }

class TransactionsClient(BaseEndpointClient):
    """ Fetch transactions """
    def __init__(self):
        super().__init__()
        self.path = '/campaign/export/cal201/transaction'
        self.by = {
            'filing': {
                'key': 'id',
                'param': 'FilingId'
            },
            'filer': {
                'key': 'localAgencyId',
                'param': 'FilerId'
            }
        }

    def fetch(self, pages=1, by='', by_data=None):
        """ loop thru all by_data,
            for each by_key, fetch self.url with param by_key
        """
        self.path += f'/{by}'
        key = self.by[by]['key']
        request_param = self.by[by]['param']

        records = []
        for i, row in enumerate(by_data):
            foreign_key = row[key]
            params = {
                **self.params,
                request_param: foreign_key
            }
            res = requests.get(self.url, headers=self.headers, params=params)
            res.raise_for_status()
            body = res.json()

            records += body[self.records_key]

            last_page = body['totalMatchingCount']
            print(f'{i} Found {last_page} total matching pages for {by} {foreign_key}')
            page_params = {
                'start_page': 1,
                'last_page': pages if pages > 0 else last_page
            }
            page = PageTracker(**page_params)
            page.print()
            page.incr()

            while page.done is False:
                res = requests.get(self.url, headers=self.headers, params={
                    **params,
                    'CurrentPageIndex': page.cur_page
                })
                res.raise_for_status()

                body = res.json()
                records += body[self.records_key]

                page.print()
                page.incr()

        return records

class BaseRecord:
    """ base class for fetching of Netfile data """
    def __init__(self):
        self.page = PageTracker()
        self.records = []
        self.endpoint = BASE_URL
        self.headers = HEADERS
        self.params = PARAMS
        self.records_key = 'results'
        self.sql_dtypes = {}
        self.df = pd.DataFrame()

    def fetch(self, pages=1):
        """ fetch one records or many """
        self.fetch_first_page()

        if pages > 0:
            self.page = PageTracker(start_page=1, last_page=pages)

        while self.page.done is False:
            res = requests.get(
                self.endpoint,
                headers=self.headers,
                params={ **self.params, 'CurrentPageIndex': self.page.cur_page }
            )
            res.raise_for_status()
            body = res.json()

            self.records += body[self.records_key]
            self.page.incr()
            self.page.print()

        return self.records

    def fetch_first_page(self):
        """ fetch the first record to get total page count """
        res = requests.get(
            self.endpoint,
            headers=self.headers,
            params=self.params
        )
        res.raise_for_status()
        body = res.json()

        self.page = PageTracker(start_page=1, last_page=body['totalMatchingPages'])
        print(f'Found {body["totalMatchingPages"]} pages')
        self.records = body[self.records_key]
        return self.records

    def to_sql(self, table_name, conn, if_exists='fail'):
        """ prepare columns for insertion into sql table """
        self.df = pd.DataFrame(self.records)
        res = self.df.to_sql(table_name, conn,
            if_exists=if_exists,
            dtype=self.sql_dtypes)
        return res

class Filing(BaseRecord):
    """ Get filings """
    def __init__(self):
        super().__init__()
        self.endpoint = f'{BASE_URL}/list/filing'
        self.records_key = 'filings'
        self.params = {
            **self.params,
            'Application': 'Campaign'
        }
        self.sql_dtypes = {
            'id': sq_types.BigInteger,
            'agency': sq_types.Integer,
            'isEfiled': sq_types.Boolean,
            'hasImage': sq_types.Boolean,
            'filingDate': sq_types.DateTime,
            'title': sq_types.String,
            'form': sq_types.Integer,
            'filerName': sq_types.String,
            'filerLocalId': sq_types.String,
            'filerStateId': sq_types.String,
            'amendmentSequenceNumber': sq_types.Integer,
            'amendedFilingId': sq_types.BigInteger
        }

class FilingTransaction(BaseRecord):
    """ Get transactions for a filing """
    def __init__(self, filing_id):
        super().__init__()
        self.endpoint = f'{BASE_URL}/campaign/export/cal201/transaction/filing'
        self.records_key = 'results'
        self.params = {
            **self.params,
            'FilingId': filing_id
        }
        self.sql_dtypes = {}

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
            page = PageTracker(last_page=num_pages)
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
    f = Filing()
    pages = 0 if get_all is True else 1
    filings = f.fetch(pages=pages)

    print('  - Collected total filings', len(filings))

    # TODO: This filitering function does not work yet.
    # TODO: Filtering is made more complicated by the fact that an amended id can appear in multiple filings
    # TODO: It is necessary to group by amendedFilingId and then get max(amendmentSequenceNumber)
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
        print(f'    - That\'s {len(amended)} after de-dupe')

        filings = [ filing for filing in filings if filing['id'] not in amended ]
        print(f'  - {len(filings)} left after filtering out amended')

    df = pd.DataFrame(filings)
    df = df.astype({
        'id': 'string',
        'title': 'string',
        'filerName': 'string',
        'filerLocalId': 'string',
        'filerStateId': 'string',
        'amendedFilingId': 'string'
    })
    df['filingDate'] = pd.to_datetime(df['filingDate'], utc=True)
    df.set_index('id', inplace=True)
    return df

def get_filing_transaction(filing_id, get_all=False):
    """ Get transactions from filing id
    """
    transaction = FilingTransaction(filing_id)
    pages = 0 if get_all is True else 1

    results = transaction.fetch(pages=pages)

    return results

def get_transactions(get_all=False, by='filing', data_by:pd.DataFrame=pd.DataFrame()):
    program = {
        'filing': {
            'function': get_filing_transaction,
            'foreign_key': 'id',
            'records_key': 'results'
        },
        'filer': {
            'function': get_filer_transactions,
            'foreign_key': 'localAgencyId',
            'records_key': 'results'
        }
    }
    
    transactions = []
    for row in data_by:
        res = program[by]['function'](row['foreign_key'], get_all=get_all)
        transactions += res['results']

    return transactions

def get_filing_transactions(filings: list[dict], get_all=False):
    """ Get all transactions for all filings """
    f = Filing()
    pages = 0 if get_all is True else 1
    filings = f.fetch(pages=pages)

    transactions = []
    for filing in filings:
        t = FilingTransaction(filing['id'])
        
        transactions += t.fetch(pages=pages)

    return transactions

def main():
    """ Collect all filings
        and see if we can find the report_num key
    """
    parser = argparse.ArgumentParser()
    endpoints_opts = [ 'transactions', 'filings' ]
    parser.add_argument('--endpoint', '-e', required=True, choices=endpoints_opts)
    parser.add_argument('--save', '-s', action='store_true')
    parser.add_argument('--all', '-a', action='store_true')
    parser.add_argument('--filter-amended', action='store_true')
    parser.add_argument('--load-database', '-d', action='store_true')
    parser.add_argument('--append', action='store_true')

    args = parser.parse_args()

    programs = {
        'transactions': {
            'function': get_transactions,
            'args': []
        },
        'filings': {
            'function': get_filings,
            'args': [ 'filter_amended' ]
        },
    }
    endpoint = args.endpoint
    print(f'Program: {endpoint}')
    program = programs[endpoint]
    kwargs = {
        'get_all': args.all,
        **{
            k.replace('-', '_'): getattr(args, k) for k in program['args']
        }
    }
    print(f'Arguments: {kwargs}')

    results = program['function'](**kwargs)
    print(f'Got {len(results)} results for endpoint {endpoint}')

    save_results = args.save or args.load_database

    if args.save is True:
        results['year'] = results['filingDate'].apply(lambda x: x.year)

        outpath = Path(args.endpoint)
        results.to_parquet(outpath, partition_cols=['year'])
        print(f'Wrote parquet to {outpath.resolve()}')

    if args.load_database is True:
        if_exists = 'replace' if not args.append else 'append'
        db_name = 'oakland_pec'
        engine = create_engine(f'postgresql+psycopg2://localhost:5432/{db_name}')

        with engine.connect() as conn:
            # snake_case all the columns
            # results.columns = [ re.sub(r'(?<!^)(?=[A-Z])', '_', c).lower() for c in results.columns ]
            print(f'- Preparing to insert columns - {results.columns}')
            res = results.to_sql(args.endpoint, conn,
                if_exists=if_exists
            )
            print(f'- Inserted {res} records into {args.endpoint} table')

    if save_results is False:
        print(results)

if __name__ == '__main__':
    main()
