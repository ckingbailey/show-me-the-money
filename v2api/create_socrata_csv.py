""" Create a CSV file from Netfile results with required fields for Socrata app

Socrata required fields:
[
  "filer_id",
  "filer_name",
  "receipt_date"
  "amount",
  "contributor_name",
  "contributor_address",
  "contributor_location",
  "contributor_type",
  "election_year",
  "jurisdiction",
  "office",
  "party",
]
"""
import json
import logging
from pathlib import Path
from random import uniform
import pandas as pd
import requests
from .query_v2_api import get_filer, AUTH

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

FILER_TO_CAND_PATH = 'filer_to_candidate.csv'
SOCRATA_CONTRIBS_SCHEMA_PATH = 'socrata_schema_contrib_fields.json'
SOCRATA_EXPEND_SCHEMA_PATH = 'socrata_schema_expend_fields.json'
EXAMPLE_DATA_DIR = 'example'
OUTPUT_DATA_DIR = 'output'

CONTRIBUTION_FORMS = [ 'F460A', 'F460C' ]
EXPENDITURE_FORM = 'F460E'
BASE_URL = 'https://netfile.com/api/campaign'
PARAMS = { 'aid': 'COAK' }
TIMEOUT = 7
SKIP_LIST = [
    '95096360-1f8d-4502-a70b-451dc6a9a0b3',
    '8deaa063-883b-4459-a32a-558653ca4fef',
    '04855230-5387-4cd9-9cfa-3e0c10fe5318'
]

class TimeoutAdapter(requests.adapters.HTTPAdapter):
    """ Will this allow me to retry on timeout? """
    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.pop('timeout', TIMEOUT)
        super().__init__(*args, **kwargs)

    def send(self, request, *args, **kwargs):
        kwargs['timeout'] = kwargs.get('timeout', self.timeout)
        return super().send(request, *args, **kwargs)

session = requests.Session()
session.hooks['response'] = [ lambda response, *args, **kwargs: response.raise_for_status() ]
retry_strategy = requests.adapters.Retry(total=5, backoff_factor=2)
adapter = TimeoutAdapter(max_retries=retry_strategy)
session.mount('https://', adapter)

def select_response_meta(response_body):
    """ Get props needed from response body for further requests """
    print(response_body['offset'], response_body['limit'])
    return {
        'page_number': response_body['pageNumber'],
        'has_next_page': response_body['hasNextPage'],
        'total': response_body['totalCount'],
        'count': response_body['count'],
        'limit': response_body['limit'],
        'offset': response_body['offset'],
        'next_offset': response_body['limit'] + response_body['offset'] if response_body['hasNextPage'] else None
    }

def get_filings(offset=0) -> tuple[pd.DataFrame, list[dict], dict]:
    """ Get a page of filings
        Return fields required by Socrata
    """
    params = { **PARAMS }

    if offset > 0:
        params['offset'] = offset

    res = session.get(f'{BASE_URL}/filing/v101/filings', params=params, auth=AUTH)
    body = res.json()

    return body['results'], select_response_meta(body)

def get_all_filings() -> list[dict]:
    """ Fetch all filings """
    filings, response_meta = get_filings()
    print(response_meta['total'])

    next_offset = response_meta['next_offset']
    end = ''
    while next_offset is not None:
        results, meta = get_filings(offset=next_offset)
        next_offset = meta['next_offset']
        filings += results
        print('¡', end=end, flush=True)
    print('')

    return filings

def get_trans() -> list[dict]:
    """ Fetch all transactions """
    params = {
        **PARAMS,
        'parts': 'All',
        'limit': 1000
    }
    offset = 0
    has_next_page = True

    results = []
    while has_next_page is True:
        if offset > 0:
            params['offset'] = offset

        try:
            res = session.get(f'{BASE_URL}/cal/v101/transaction-elements', params=params, auth=AUTH)
        except requests.HTTPError as exc:
            print(f'{exc.response.status_code} for request {exc.response.url}')
            params_no_parts = { ** params }
            params_no_parts.pop('parts')
            res = session.get(f'{BASE_URL}/cal/v101/transaction-elements', params=params_no_parts, auth=AUTH)

        body = res.json()
        results = results + body['results']

        has_next_page = body['hasNextPage']
        offset = offset + body['limit']
        print('\u258a', end='', flush=True)

    return results

def get_trans_for_filing(filing_nid, offset=0) -> tuple[list[dict], dict]:
    """ Get a page of transactions
        for a filingNid
    """
    params = {
        **PARAMS,
        'filingNid': filing_nid,
        'parts': 'All'
    }

    if offset > 0:
        params['offset'] = offset

    res = session.get(f'{BASE_URL}/cal/v101/transaction-elements', params=params, auth=AUTH)
    body = res.json()

    return body['results'], select_response_meta(body)

def get_all_trans_for_filing(filing_nid):
    """ Get all transactions for a single filing_nid """
    next_offset = 0
    params = {
        'filing_nid': filing_nid
    }

    transactions, meta = get_trans_for_filing(**params)
    end = '/' if meta['total'] > meta['limit'] else ' '
    if end != ' ':
        print('')
    print(meta['total'], end=end, flush=True)

    next_offset = meta.get('next_offset')
    while next_offset is not None:
        results, meta = get_trans_for_filing(**params, offset=next_offset)
        next_offset = meta.get('next_offset')
        prog_char = '¡' if len(results) > 0 else '.'
        transactions += results
        print(next_offset, end='', flush=True)

    return transactions

def get_trans_for_filings(filing_nids: set) -> list[dict]:
    """ Get all transactions for set of filing netfile IDs """
    transactions = []
    for filing_nid in filing_nids:
        if filing_nid in SKIP_LIST:
            continue
        transactions += get_all_trans_for_filing(filing_nid)
    print('')

    return transactions

def get_all_filers(filer_nids: set) -> list[dict]:
    """ Fetch all filers """
    filers = []
    for filer_nid in filer_nids:
        filers += get_filer(filer_nid)
        print('¡', end='', flush=True)
    print('')

    return filers

def df_from_filings(filings):
    """ Transform filings into Pandas DataFrame """
    return pd.DataFrame([{
        'filer_nid': f['filerMeta']['filerId'],
        'filing_nid': f['filingNid'],
        'filing_date': f['calculatedDate'],
        'form': f['specificationRef']['name'].replace('FPPC', ''),
        'committee_name': f['filerMeta']['commonName']
    } for f in filings ])

def get_address(addresses: list[dict]) -> dict[str, str]:
    """ Get street address from addresses, or return empty string """
    if len(addresses) < 1:
        return {
            'contributor_address': '',
            'city': '',
            'state': '',
            'zip_code': ''
        }

    address = addresses[0]

    street = f'{address["line1"] or ""} {address["line2"] or ""}'.strip()

    return {
        'contributor_address': ', '.join([
            street,
            ' '.join([
                address['city'],
                address['state'],
                address['zip']
            ])
        ]),
        'city': address['city'],
        'state': address['state'],
        'zip_code': address['zip']
    }

def get_location(addresses):
    """ Get (long, lat) from addresses, or return empty string """
    if len(addresses) <= 0:
        return ''

    address = addresses[0]
    long = address['longitude']
    lat = address['latitude']

    if long is None or lat is None:
        return ''

    # long_range = (0.2275, 0.455) # approx. b/w .25 mi and .5 mi @ 38ºN
    # lat_range = (0.2173, 0.575) # approx. b/w .25 mi and .5 mi @ 38ºN
    long_range = 0,0
    lat_range = 0,0
    adjusted = [str(float(long) + uniform(*long_range)), str(float(lat) + uniform(*lat_range))]
    return f'POINT ({" ".join(adjusted)})'

def get_contrib_category(entity_code):
    """ Translate three-letter entityCd into human readable entity code """
    return {
        'RCP': 'Committee',
        'IND': 'Individual',
        'OTH': 'Business/Other',
        'COM': 'Committee',
        'PTY': 'Political Party',
        'SCC': 'Small Contributor Committee'
    }.get(entity_code)

def df_from_trans(transactions):
    """ Transform transaction dict into Pandas DataFrame """
    tran_cols = [
        'tran_id',
        'filing_nid',
        'contributor_name',
        'contributor_type',
        'contributor_category',
        'contributor_address',
        'city',
        'state',
        'zip_code',
        'contributor_location',
        'amount',
        'receipt_date',
        'expn_code',
        'expenditure_description',
        'form',
        'party'
    ]

    transaction_data = [
        {
            'tran_id': t['transaction']['tranId'],
            'filing_nid': t['filingNid'],
            'contributor_name': t['allNames'],
            'contributor_type': 'Individual' if t['transaction']['entityCd'] == 'IND' else 'Organization',
            'contributor_category': get_contrib_category(t['transaction']['entityCd']),
            **get_address(t['addresses']),
            'contributor_location': None,
            'amount': t['calculatedAmount'],
            'receipt_date': t['transaction']['tranDate'],
            'expn_code': t['transaction']['tranCode'],
            'expenditure_description': t['transaction']['tranDscr'] or '',
            'form': t['calTransactionType'],
            'party': None,
        } for t in transactions
        if t.get('transaction') is not None # Skip incomplete transactions
    ]

    df = pd.DataFrame(transaction_data, columns=tran_cols)
    df['receipt_date'] = pd.to_datetime(df['receipt_date'])
    return df

def df_from_filers(filers):
    """ Transform filers into Pandas DataFrame """
    # filter out committees without CA SOS IDs
    # as we have no way to join them to filings
    return pd.DataFrame([ {
        'filer_nid': f['filerNid'],
        'filer_id': f['registrations'].get('CA SOS')
    } for f in filers if f['registrations'].get('CA SOS') is not None
    ]).astype({ 'filer_id': 'string' })

def get_jurisdiction(row):
    """ Get jurisdiction of office, one of
        - Council District
        - OUSD District
        - Citywide
    """
    if row['office'].startswith('City Council District '):
        return 'Council District'
    if row['office'].startswith('OUSD District'):
        return 'Oakland Unified School District'
    
    return 'Citywide'

def save_source_data(json_data: list[dict]) -> None:
    """ Save JSON data output from NetFile API """
    for endpoint_name, data in json_data.items():
        Path(f'{EXAMPLE_DATA_DIR}/{endpoint_name}.json').write_text(
            json.dumps(data, indent=4
        ), encoding='utf8')

def main():
    """ Query Netfile results 1 page at a time
        Build Pandas DataFrame
        and then save it as CSV

        0. Get all elections, collect dates into ordered list
        1. Query filing
        2. For each filing, query transaction-elements?filingNid={filingNid}&parts=All
        3. Match filingDate to electionDate, extract year from date
        4. Query /filer/v101/filers/{filer_nid}, get electionInfluences[electionDate].seat.officeName
    """
    print('===== Get filings =====')
    filings = get_all_filings()

    filing_df = df_from_filings(filings)
    filing_df['filing_date'] = pd.to_datetime(filing_df['filing_date'])

    print('===== Get transactions =====')
    filing_nids = set(filing_df['filing_nid'])
    transactions = get_trans()
    print('Number of transaction objects with "transaction" props',
        len([ tran for t in transactions if (tran := t.get('transaction')) is not None ]))

    print('===== Get filers =====')
    filers = get_all_filers(set(filing_df['filer_nid']))

    tran_df = df_from_trans(transactions)
    filer_df = df_from_filers(filers)

    expn_codes = pd.read_csv('expenditure_codes.csv').rename(columns={
        'description': 'expenditure_type'
    })
    print('===== expn_codes dtypes', expn_codes.dtypes, sep='\n')
    tran_df = tran_df.merge(expn_codes, how='left', on='expn_code')
    print('===== tran_df dtypes =====', len(tran_df.index), tran_df.dtypes, sep='\n')

    save_source_data({
        'filings': filings,
        'transactions': transactions,
        'filers': filers
    })

    filer_to_cand_cols = [
        'local_agency_id',
        'filer_id',
        'election_year',
        'filer_name',
        'filer_name_local',
        'office',
        'start_date',
        'end_date'
    ]
    filer_to_cand = pd.read_csv(FILER_TO_CAND_PATH)
    filer_to_cand = filer_to_cand.rename(columns={
        'SOS ID': 'filer_id',
        'Local Agency ID': 'local_agency_id',
        'Filer Name': 'filer_name_local',
        'contest': 'office',
        'candidate': 'filer_name',
        'start': 'start_date',
        'end': 'end_date'
    })[
        filer_to_cand_cols
    ].astype({
        'filer_id': 'string'
    })
    filer_to_cand['jurisdiction'] = filer_to_cand.apply(get_jurisdiction, axis=1)
    filer_to_cand['end_date'] = pd.to_datetime(filer_to_cand['end_date'])
    filer_to_cand['start_date'] = pd.to_datetime(filer_to_cand['start_date'])

    df = filer_to_cand.merge(filer_df, how='left', on='filer_id')
    df = df.merge(
        filing_df.drop(columns=['form']), how='left', on='filer_nid'
    ).merge(
        tran_df, on='filing_nid')
    df = df.astype({
        'filer_name': 'string',
        'contributor_name': 'string',
        'contributor_type': 'string',
        'contributor_address': 'string',
        'amount': float
    }).rename(columns={
        'filing_nid': 'filing_id'
    })
    df['filer_name'] = df['filer_name'].apply(lambda n: n.strip())

    pd.set_option('max_colwidth', 12)
    print(df.head(12))

    df.to_csv(f'{EXAMPLE_DATA_DIR}/all_trans.csv', index=False)

    common_cols = [ 'city', 'state', 'zip_code', 'committee_name', 'filing_id', 'tran_id' ]
    contrib_extra_cols = [ 'contributor_category' ]
    contrib_socrata_schema = json.loads(
        Path(SOCRATA_CONTRIBS_SCHEMA_PATH).read_text(encoding='utf8')
    ).keys()
    contrib_cols = [
        'tran_id',
        'filing_id',
        'filer_id',
        'filer_name',
        'committee_name',
        'contributor_name',
        'contributor_type',
        'contributor_category',
        'contributor_address',
        'contributor_location',
        'city',
        'state',
        'zip_code',
        'amount',
        'receipt_date',
        'election_year',
        'office',
        'jurisdiction',
        'party'
    ]
    contrib_df = df[df['form'].isin(CONTRIBUTION_FORMS)]
    contrib_df = contrib_df[
        (contrib_df['end_date'].isna())
        | (contrib_df['receipt_date'] < contrib_df['end_date'])
    ][contrib_cols]
    print(contrib_df.head(), len(contrib_df.index), sep='\n')
    contrib_df.to_csv(f'{OUTPUT_DATA_DIR}/contribs_socrata.csv', index=False)

    expend_cols = (json.loads(Path(SOCRATA_EXPEND_SCHEMA_PATH).read_text(encoding='utf8'))
    + common_cols)
    expend_df = df[df['form'] == EXPENDITURE_FORM].rename(columns={
        'contributor_name': 'recipient_name',
        'contributor_address': 'recipient_address',
        'contributor_location': 'recipient_location',
        'receipt_date': 'expenditure_date'
    })[expend_cols]
    print(expend_df.head(), len(expend_df.index), sep='\n')
    expend_df.to_csv(f'{OUTPUT_DATA_DIR}/expends_socrata.csv', index=False)

if __name__ == '__main__':
    main()
