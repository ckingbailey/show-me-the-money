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
from pathlib import Path
from random import uniform
import pandas as pd
import requests
from .query_v2_api import get_filer

FILER_TO_CAND_PATH = 'filer_to_candidate.csv'
SOCRATA_CONTRIBS_SCHEMA_PATH = 'socrata_schema_contrib_fields.json'
SOCRATA_EXPEND_SCHEMA_PATH = 'socrata_schema_expend_fields.json'
EXAMPLE_DATA_DIR = 'example'
OUTPUT_DATA_DIR = 'output'

CONTRIBUTION_FORMS = [ 'F460A', 'F460C' ]
EXPENDITURE_FORM = 'F460E'
BASE_URL = 'https://netfile.com/api/campaign'
PARAMS = { 'aid': 'COAK' }
AUTH = tuple(v for k,v in sorted(
    [ ln.split('=') for ln in Path('.env').read_text(encoding='utf8').strip().split('\n') ],
    key=lambda r: [ 'api_key', 'api_secret'].index(r[0])
))
TIMEOUT = 7

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
retry_strategy = requests.adapters.Retry(total=5, backoff_factor=1)
adapter = TimeoutAdapter(max_retries=retry_strategy)
session.mount('https://', adapter)

def select_response_meta(response_body):
    """ Get props needed from response body for further requests """
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

def get_transactions(filing_nid, offset=0) -> tuple[pd.DataFrame, list[dict], dict]:
    """ Get a page of transactions
        for a filingNid
        Return fields required by Socrata
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

    transactions, meta = get_transactions(**params)
    end = '/' if meta['total'] > meta['limit'] else ' '
    if end != ' ':
        print('')
    print(meta['total'], end=end, flush=True)

    next_offset = meta.get('next_offset')
    while next_offset is not None:
        results, meta = get_transactions(**params, offset=next_offset)
        prog_char = '¡' if len(results) > 0 else '.'
        transactions += results
        print(prog_char, end='', flush=True)

    return transactions

def df_from_filings(filings):
    """ Transform filings into Pandas DataFrame """
    return pd.DataFrame([{
        'filer_nid': f['filerMeta']['filerId'],
        'filing_nid': f['filingNid'],
        'receipt_date': f['calculatedDate'],
        'committee_name': f['filerMeta']['commonName']
    } for f in filings ])

def get_location(addresses):
    """ Get (long, lat) from addresses, or return empty string """
    if len(addresses) <= 0:
        return ''

    address = addresses[0]
    long = address['longitude']
    lat = address['latitude']

    if long is None or lat is None:
        return ''

    long_range = (0.2275, 0.455) # approx. b/w .25 mi and .5 mi @ 38ºN
    lat_range = (0.2173, 0.575) # approx. b/w .25 mi and .5 mi @ 38ºN
    adjusted = [str(float(long) + uniform(*long_range)), str(float(lat) + uniform(*lat_range))]
    return f'POINT{" ".join(adjusted)}'

def get_address(addresses):
    """ Get street address from addresses, or return empty string """
    if len(addresses) < 1:
        return ''

    address = addresses[0]

    street = f'{address["line1"] or ""} {address["line2"] or ""}'.strip()

    return ', '.join([
        street,
        ' '.join([
            address['city'],
            address['state'],
            address['zip']
        ])
    ])

def df_from_trans(transactions):
    """ Transform transaction dict into Pandas DataFrame """
    tran_cols = [
        'tran_id',
        'filing_nid',
        'contributor_name',
        'contributor_type',
        'contributor_address',
        'contributor_location',
        'amount',
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
            'contributor_address': get_address(t['addresses']),
            'contributor_location': get_location(t['addresses']),
            'amount': t['calculatedAmount'],
            'expn_code': t['transaction']['tranCode'],
            'expenditure_description': t['transaction']['tranDscr'] or '',
            'form': t['calTransactionType'],
            'party': None,
        }
        for t in transactions
    ]

    return pd.DataFrame(transaction_data, columns=tran_cols)

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
    if row['office'].startswith('City Council District '):
        return 'Council District'
    if row['office'].startswith('OUSD District'):
        return 'Oakland Unified School District'
    
    return 'Citywide'

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

    filing_df = df_from_filings(filings)

    # Keep only filings for 2020
    # This is bogus because 2020 election filings may have been received in 2019
    # Get the ca_sos_id => election_date mapping from Suzanne
    filing_df['receipt_date'] = pd.to_datetime(filing_df['receipt_date'])
    filing_df = filing_df[filing_df['receipt_date'].dt.year > 2018]

    print('===== Get transactions =====')
    filing_nids = set(filing_df['filing_nid'])
    transactions = []
    for filing_nid in filing_nids:
        transactions += get_all_trans_for_filing(filing_nid)
    print('')

    print('===== Get filers =====')
    filers = []
    for filer_nid in set(filing_df['filer_nid']):
        filers += get_filer(filer_nid)
        print('¡', end='', flush=True)
    print('')

    tran_df = df_from_trans(transactions)
    filer_df = df_from_filers(filers)

    expn_codes = pd.read_csv('expenditure_codes.csv').rename(columns={
        'description': 'expenditure_type'
    })
    print('===== expn_codes dtypes', expn_codes.dtypes, sep='\n')
    tran_df = tran_df.merge(expn_codes, how='left', on='expn_code')
    print('===== tran_df dtypes =====', len(tran_df.index), tran_df.dtypes, sep='\n')

    Path(f'{EXAMPLE_DATA_DIR}/filings.json').write_text(json.dumps(filings, indent=4), encoding='utf8')
    Path(f'{EXAMPLE_DATA_DIR}/transactions_2019-present.json').write_text(
        json.dumps(transactions, indent=4), encoding='utf8'
    )
    Path(f'{EXAMPLE_DATA_DIR}/filers_2019-present.json').write_text(json.dumps(filers, indent=4), encoding='utf8')

    filer_to_cand_cols = [
        'local_agency_id',
        'filer_id',
        'election_year',
        'filer_name',
        'filer_name_local',
        'office',
        'start',
        'end'
    ]
    df = pd.read_csv(FILER_TO_CAND_PATH)
    df = df.rename(columns={
        'SOS ID': 'filer_id',
        'Local Agency ID': 'local_agency_id',
        'Filer Name': 'filer_name_local',
        'contest': 'office',
        'candidate': 'filer_name'
    })[
        filer_to_cand_cols
    ].astype({ 'filer_id': 'string' })
    print('¿ What happened to office col?', df.columns)
    df['jurisdiction'] = df.apply(get_jurisdiction, axis=1)

    df = df.merge(filer_df, how='left', on='filer_id')
    df = df.merge(filing_df, how='left', on='filer_nid').merge(tran_df, on='filing_nid')
    df = df.astype({
        'filer_name': 'string',
        'contributor_name': 'string',
        'contributor_type': 'string',
        'contributor_address': 'string',
        'amount': float
    }).rename(columns={
        'filing_nid': 'filing_id'
    })
    print('===== dtypes after merge =====', len(df.index), df.dtypes, sep='\n')
    pd.set_option('max_colwidth', 12)
    print(df.head(12))

    df.to_csv(f'{EXAMPLE_DATA_DIR}/all_trans.csv', index=False)

    contrib_cols = (list(json.loads(
        Path(SOCRATA_CONTRIBS_SCHEMA_PATH).read_text(encoding='utf8')
    ).keys()) + [ 'committee_name', 'filing_id', 'tran_id' ])
    contrib_df = df[df['form'].isin(CONTRIBUTION_FORMS)][contrib_cols]
    print(contrib_df.head(), len(contrib_df.index), sep='\n')
    contrib_df.to_csv(f'{OUTPUT_DATA_DIR}/contribs_socrata.csv', index=False)

    expend_cols = (json.loads(Path(SOCRATA_EXPEND_SCHEMA_PATH).read_text(encoding='utf8'))
    + [ 'committee_name', 'filing_id', 'tran_id' ])
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
