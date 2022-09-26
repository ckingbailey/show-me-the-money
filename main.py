""" Oakland PEC Netfile data exploration
"""
import argparse
from pathlib import Path
import re
from time import sleep
from xmlrpc.client import DateTime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from sqlalchemy import create_engine, types as sql_types # pylint: disable=import-error

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
        self.records_class = BaseRecord

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
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1.0)
        session.mount('https://', HTTPAdapter(max_retries=retries))
        res = session.get(
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
            res = session.get(
                url,
                headers=self.headers,
                params=params
            )
            res.raise_for_status()
            body = res.json()

            records += body[records_key]
            page.print()
            page.incr()

        return self.records_class(records)

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
        self.records_class = Filing

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
        self.records_class = Transaction

    def fetch(self, pages=1, by='', by_data=None):
        """ loop thru all by_data,
            for each by_key, fetch self.url with param by_key
        """
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1.0)
        session.mount('https://', HTTPAdapter(max_retries=retries))

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
            res = session.get(self.url, headers=self.headers, params=params)
            res.raise_for_status()
            body = res.json()

            records += body[self.records_key]

            last_page = body['totalMatchingCount']
            print(f'{i}. Found {last_page} total matching pages for {by} {foreign_key}')
            page_params = {
                'start_page': 1,
                'last_page': pages if pages > 0 else last_page
            }
            page = PageTracker(**page_params)
            page.print()

            while page.done is False:
                page.incr()
                res = session.get(self.url, headers=self.headers, params={
                    **params,
                    'CurrentPageIndex': page.cur_page
                })
                res.raise_for_status()

                body = res.json()
                records += body[self.records_key]

                page.print()

        return self.records_class(records)

class BaseRecord:
    """ base class for fetching of Netfile data """
    def __init__(self, records):
        self.records = records
        self.sql_dtypes = {}
        self.df = pd.DataFrame()
        self.table_name = ''

    def to_sql(self, conn, if_exists='fail'):
        """ prepare columns for insertion into sql table """
        self.df = pd.DataFrame(self.records)
        print(f'- Preparing to insert columns - {self.df.columns}')
        res = self.df.to_sql(self.table_name, conn,
            if_exists=if_exists,
            index=False,
            dtype=self.sql_dtypes)
        return res

class Filing(BaseRecord):
    """ Get filings """
    def __init__(self, records):
        super().__init__(records)
        self.table_name = 'filings'
        self.sql_dtypes = {
            'id': sql_types.BigInteger,
            'agency': sql_types.Integer,
            'isEfiled': sql_types.Boolean,
            'hasImage': sql_types.Boolean,
            'filingDate': sql_types.DateTime,
            'title': sql_types.String,
            'form': sql_types.Integer,
            'filerName': sql_types.String,
            'filerLocalId': sql_types.String,
            'filerStateId': sql_types.String,
            'amendmentSequenceNumber': sql_types.Integer,
            'amendedFilingId': sql_types.BigInteger
        }

class Transaction(BaseRecord):
    """ Get transactions for a filing """
    def __init__(self, records):
        super().__init__(records)
        self.table_name = 'transactions'
        self.sql_dtypes = {
            "filingId": sql_types.BigInteger
        }
        all_columns = {
            'amountType',
            'amt_Incur',
            'amt_Paid',
            'bakRef_TID',
            'bal_Juris',
            'bal_Name',
            'bal_Num',
            'beg_Bal',
            'calculated_Amount',
            'calculated_Date',
            'cand_NamF',
            'cand_NamL',
            'cand_NamS',
            'cand_NamT',
            'cmte_Id',
            'dist_No',
            'elec_Date',
            'end_Bal',
            'entity_Cd',
            'externalId',
            'filerLocalId',
            'filerName',
            'filerStateId',
            'filingEndDate',
            'filingId',
            'filingStartDate',
            'form_Type',
            'g_From_E_F',
            'int_CmteId',
            'int_Rate',
            'intr_Adr1',
            'intr_Adr2',
            'intr_City',
            'intr_Emp',
            'intr_NamF',
            'intr_NamL',
            'intr_NamS',
            'intr_NamT',
            'intr_Occ',
            'intr_ST',
            'intr_Self',
            'intr_Zip4',
            'juris_Cd',
            'juris_Dscr',
            'latitude',
            'lender_Name',
            'loan_Amt1',
            'loan_Amt2',
            'loan_Amt3',
            'loan_Amt4',
            'loan_Amt5',
            'loan_Amt6',
            'loan_Amt7',
            'loan_Amt8',
            'loan_Date1',
            'loan_Date2',
            'loan_Rate',
            'longitude',
            'memo_Code',
            'memo_RefNo',
            'netFileKey',
            'off_S_H_Cd',
            'office_Cd',
            'office_Dscr',
            'rec_Type',
            'sup_Opp_Cd',
            'tran_Adr1',
            'tran_Adr2',
            'tran_Amt1',
            'tran_Amt2',
            'tran_ChkNo',
            'tran_City',
            'tran_Code',
            'tran_Date',
            'tran_Date1',
            'tran_Dscr',
            'tran_Emp',
            'tran_Id',
            'tran_NamF',
            'tran_NamL',
            'tran_NamS',
            'tran_NamT',
            'tran_Occ',
            'tran_ST',
            'tran_Self',
            'tran_Type',
            'tran_Zip4',
            'transactionType',
            'tres_Adr1',
            'tres_Adr2',
            'tres_City',
            'tres_NamF',
            'tres_NamL',
            'tres_NamS',
            'tres_NamT',
            'tres_ST',
            'tres_Zip4',
            'xref_Match',
            'xref_SchNum'
        }

class RecordCollection:
    """ For handling a variety of records arrays """
    def __init__(self, *records):
        self.records = [ *records ]

    def to_sql(self, conn, if_exists='fail'):
        """ Handle writing multiple collections of recordss to sql tables """
        responses = []
        for record_type in self.records:
            res = record_type.to_sql(conn, if_exists=if_exists)
            responses.append(res)

        return responses

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
        if len(transactions) > 0:
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

def get_filings(get_all=False, filter_amended=False) -> Filing:
    """ Collect filings, return Pandas DataFrame
    """
    # Collect Campaign filings
    f = NetfileClient().filings
    pages = 0 if get_all is True else 1
    filings = f.fetch(pages=pages)

    print('  - Collected total filings', len(filings.records))

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

    return filings

def get_transactions(get_all=False, by='filing', data_by:list[dict]=None) -> RecordCollection:
    """ Get transactions by either filing or filer """
    program = {
        'filing': {
            'function': get_filing_transactions,
            'foreign_key': 'id',
            'records_key': 'results'
        },
        'filer': {
            'function': get_filer_transactions,
            'foreign_key': 'localAgencyId',
            'records_key': 'results'
        }
    }
    
    return program[by]['function'](data_by, get_all=get_all)

def get_filing_transactions(filings: list[dict], get_all=False) -> RecordCollection:
    """ Get all transactions for all filings """
    netfile = NetfileClient()
    f = netfile.filings
    pages = 0 if get_all is True else 1
    filings = f.fetch(pages=pages)

    t = netfile.transactions
    transactions = t.fetch(pages, by='filing', by_data=filings.records)
    return RecordCollection(transactions, filings)

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
    # print(f'Got {len(results)} results for endpoint {endpoint}')

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
            res = results.to_sql(conn, if_exists=if_exists)
            print(f'- Inserted {res} records into {args.endpoint} table')

    if save_results is False:
        print(results)

if __name__ == '__main__':
    main()
