import argparse
from ast import arg
from itertools import zip_longest
import json
from pathlib import Path
from pprint import PrettyPrinter
from textwrap import fill
from typing import Iterable
from unittest.mock import Base
import pandas as pd
from .create_socrata_csv import (
    AUTH,
    PARAMS,
    BASE_URL,
    session,
    save_source_data,
    df_from_candidates,
    df_from_filings,
    df_from_filers,
    get_contrib_category,
    get_address)

FILING_ELEMENTS_NAME = 'filing_elements'

pp = PrettyPrinter()

class Routes:
    """ NetFile routes """
    filings = '/filing/v101/filings'
    filers = '/filer/v101/filers'
    transactions = '/cal/v101/transaction-elements'
    filing_activities = 'filing/v101/filing-activities'
    filing_elements = '/filing/v101/filing-elements'

class BaseEndpointClient:
    """ Base functionality for fetching from NetFile endpoint """
    def __init__(self, base_url, base_params, auth):
        self.has_next_page = True
        self.base_url = base_url
        self.base_params = base_params
        self.auth = auth
        self.session = session

class FilingElementsClient(BaseEndpointClient):
    """ Fetch filing elements """
    def __init__(self, base_url, base_params, auth, **kwargs):
        super().__init__(base_url, base_params, auth)
        route = Routes.filing_elements
        self.url = f'{self.base_url}{route}'
        self.params = {
            **self.base_params,
            'offset': 0,
            'limit': 1000
        }
        self._fetchable_by = [
            'filing_nid',
            'element_nid'
        ]

        # Prepare own query attributes
        self._set_next_fetch_by = self._configure_fetch_by(**kwargs)

    def _configure_next_request(self, res):
        self.has_next_page = res['hasNextPage']
        next_offset = self.params['limit'] + self.params['offset'] if self.has_next_page else None

        if next_offset is None:
            try:
                self._set_next_fetch_by()
            except StopIteration:
                pass
        else:
            self.params['offset'] = next_offset

    def _get(self, params):
        res = self.session.get(self.url, auth=self.auth, params=params)
        body = res.json()
        print('.', end='', flush=True)
        self._configure_next_request(body)

        return body['results']

    def fetch(self):
        """ Fetch all filings elements,
            probably for filing_nids
        """
        return self._get(self.params)

    def _configure_fetch_by(self, **kwargs):
        if len(kwargs) > 1:
            raise ValueError(f'filing_elements may only be queried by 1 param type, got {kwargs}')

        fetch_by = list(kwargs.keys())[0]
        if fetch_by not in self._fetchable_by:
            raise ValueError(f'Unknown request parameter {fetch_by}')

        return getattr(self, f'_set_next_{fetch_by}')(kwargs[fetch_by])

    def _set_next_filing_nid(self, filing_nids:list[str]=None):
        """ Add 'filing_nid' to self.params """
        filing_nids_iter = iter(filing_nids)
        filing_nid = next(filing_nids_iter)
        fetch_by = 'filingNid'
        def set_next():
            self.params[fetch_by] = filing_nid

        return set_next

    def _set_next_element_nid(self, element_nid):
        """ Add /{element_nid} to self.url """
        pass

class FilingClient(BaseEndpointClient):
    pass

class FilerClient(BaseEndpointClient):
    pass

class TransactionsClient(BaseEndpointClient):
    pass

class FilingActivityClient(BaseEndpointClient):
    pass

class NetFileClient:
    """ Fetch data from NetFile V2 endpoints """
    base_url = BASE_URL
    base_params = PARAMS
    auth = AUTH
    routes = Routes
    session = session
    _fetcher = {
        'filing_elements': FilingElementsClient,
        'filings': FilingClient,
        'filer': FilerClient,
        'transaction': TransactionsClient,
        'filing_activities': FilingActivityClient
    }

    @classmethod
    def fetch(cls, endpoint, **kwargs):
        """ Fetch all of a particular record type """
        fetcher = cls._fetcher[endpoint](cls.base_url, cls.base_params, cls.auth, **kwargs)
        results = []

        while fetcher.has_next_page:
            results += fetcher.fetch()

        print('')
        return results

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

def get_multiple_filing_elements(filings:pd.DataFrame) -> pd.DataFrame:
    """ Get all filing elements for a list of filings """
    filing_elements = []
    for filing_nid in filings['filing_nid']:
        filing_elements += get_filing_elements_by_filing(filing_nid)

    return filing_elements

def load_from_file(filepath:str) -> list[dict]:
    """ Load json file from file name, without extension, within example/ folder """
    return json.loads(Path(f'example/{filepath}.json').read_text(encoding='utf8'))

def load_filings() -> pd.DataFrame:
    """ Load filings from disk """
    return df_from_filings(load_from_file('filings'))

def load_filers() -> pd.DataFrame:
    """ Load filers from disk """
    return df_from_filers(load_from_file('filers'))

def load_filing_elements():
    """ Load filing elements from disk """
    return load_from_file(FILING_ELEMENTS_NAME)

def get_unitemized_trans_for_filings(filings, filing_elements):
    """ Get unitemized transaction amounts by date
        from filings
    """
    filing_parts = {}
    filing_dates = {}
    for f in filing_elements:
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

    return {
        d: f['elementModel']['scheduleA']['line2']
        for d, parts
        in filing_parts.items()
            for f in parts
            if f['elementActivityType'] != 'Superseded'
            and f['elementClassification'] == 'Summary'
    }

def get_filer_nid_from_name_and_election(
    last_name: str,
    election_year: int,
    filers: list[dict]) -> str:
    """ Get NetFile filerNid from candidate last name and election year """
    filer = [ f for f in filers if last_name in f['filerName'] and election_year in f['filerName'] ][0]
    return filer['filerNid']

def get_filings_for_filer(filer_nid:str, filings:list[dict]) -> list[dict]:
    """ Get all filings for one filer_nid """
    return [ f for f in filings if str(f['filerMeta']['filerId']) == str(filer_nid) ]

def get_filings_for_filers(filers:Iterable[str], filings:list[dict]) -> list[dict]:
    """ Get all filings for a list of filerNids """
    return [
        f for f
        in filings
        if str(f['filerMeta']['filer_id']) in filers
    ]

class Transaction:
    """ A transaction record """
    def __init__(self, transaction_record: dict):
        transaction_model = transaction_record['transaction']

        self.element_nid = transaction_record['elementNid']
        self.tran_id = transaction_model['tranId']
        self.filing_nid = transaction_record['filingNid']

        transactor_first_name = transaction_model['tranNamF'] or ''
        transactor_last_name = transaction_model['tranNamL'] or ''
        contributor_name = (
            transaction_record.get('allNames')
            or f'{transactor_first_name} {transactor_last_name}'.strip())
        self.contributor_name = contributor_name

        self.contributor_type = ('Individual'
            if transaction_model['entityCd'] == 'IND'
            else 'Organization')
        self.contributor_category = get_contrib_category(transaction_model['entityCd'])
        self.contributor_location = None
        self.amount = transaction_model['tranAmt1']
        self.receipt_date = transaction_model['tranDate']
        self.expn_code = transaction_model['tranCode']
        self.expenditure_description = transaction_model['tranDscr'] or ''
        self.form = transaction_model['calTransactionType']
        self.party = None

        self.contributor_address = None
        self.city = None
        self.state = None
        self.zip_code = None
        self.contributor_region = None

        self.get_address(transaction_model)

    @classmethod
    def from_unitemized(cls, unitemized: dict):
        """ Create a Transaction record from an "UnItemized" filing-element record """
        return cls({
            **unitemized,
            'transaction': {
                'tranId': 'Unitemized',
                'entityCd': 'Unitemized',
                'tranDate': unitemized['elementModel']['calculatedDate'],
                'tranCode': 'Unitemized',
                'tranDscr': 'Unitemized',
                'tranNamF': '',
                'tranNamL': 'Unitemized',
                'tranAdr1': '',
                'tranAdr2': '',
                'tranCity': '',
                'tranST': '',
                'tranZip4': '',
                'tranAmt1': unitemized['elementModel']['amount'],
                'calTransactionType': unitemized['specificationRef']['name']
            }
        })

    def get_address(self, transaction_model: dict) -> None:
        """ Set address fields """
        address = get_address([{
            "line1": transaction_model['tranAdr1'],
            "line2": transaction_model['tranAdr2'],
            "city": transaction_model['tranCity'],
            "state": transaction_model['tranST'],
            "zip": transaction_model['tranZip4']
        }])
        self.contributor_address = address['contributor_address']
        self.city = address['city']
        self.state = address['state']
        self.zip_code = address['zip_code']
        self.contributor_region = address['contributor_region']

    @property
    def df(self):
        """ Get it as a Pandas DataFrame """
        df = pd.DataFrame([self.__dict__])
        df['receipt_date'] = pd.to_datetime(df['receipt_date'])

        return df

def main():
    """ Do whatever I'm currently working on """
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true')
    args = parser.parse_args()

    filings = load_filings()

    filers = load_filers()

    filer_to_cand = df_from_candidates()
    filer_id_mapping = filer_to_cand.merge(filers, how='left', on='filer_id')

    candidate_filings = filings.merge(filer_id_mapping, how='right', on='filer_nid')
    print('num candidate filings', len(candidate_filings))

    if args.download:
        filing_elements = NetFileClient.fetch(
            'filing_elements',
            filing_nid=candidate_filings['filing_nid']
        )
        save_source_data({ FILING_ELEMENTS_NAME: filing_elements })
    else:
        filing_elements = load_filing_elements()

    print('num filing elements', len(filing_elements))

    transaction_elements = [
        Transaction({
            **f,
            'transaction': f['elementModel']
        }) for f
        in filing_elements
        if f['elementClassification'] == 'Transaction'
        and f['elementActivityType'] != 'Superseded'
    ]
    print('num transaction elements', len(transaction_elements))
    pp.pprint(transaction_elements[0])

    unitemized_elements = [
        Transaction.from_unitemized(f) for f
        in filing_elements
        if f['elementClassification'] == 'UnItemizedTransaction'
        and f['elementType'] == 'F460ALine2'
        and f['elementActivityType'] != 'Superseded'
    ]
    print('num unitemized transaction elements', len(unitemized_elements))
    pp.pprint(unitemized_elements[0].df)

if __name__ == '__main__':
    main()
