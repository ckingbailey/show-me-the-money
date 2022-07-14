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
from pathlib import Path
import pandas as pd
import requests

BASE_URL = 'https://netfile.com/api/campaign'
PARAMS = { 'aid': 'COAK' }
AUTH = tuple(v for k,v in sorted(
    [ ln.split('=') for ln in Path('.env').read_text(encoding='utf8').strip().split('\n') ],
    key=lambda r: [ 'api_key', 'api_secret'].index(r[0])
))
TIMEOUT = 7

session = requests.Session()
session.hooks['response'] = [ lambda response, *args, **kwargs: response.raise_for_status() ]
retry_strategy = requests.adapters.Retry(total=3, backoff_factor=1)
adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
session.mount('https://', adapter)

def get_filings(offset=0) -> tuple[pd.DataFrame, dict]:
    """ Get a page of filings
        Return fields required by Socrata
    """
    params = { **PARAMS }

    if offset > 0:
        params['offset'] = offset

    res = session.get(f'{BASE_URL}/filing/v101/filings', params=params, auth=AUTH, timeout=TIMEOUT)
    body = res.json()

    return pd.DataFrame([
        {
            'filer_id': result['filerMeta']['filerId'],
            'filer_name': result['filerMeta']['commonName'],
            'filing_nid': result['filingNid'],
            'receipt_date': result['calculatedDate']
        } for result in body['results']
    ]), {
        'page_number': body['pageNumber'],
        'has_next_page': body['hasNextPage'],
        'total': body['totalCount'],
        'count': body['count'],
        'offset': body['offset'],
        'next_offset': body['count'] + body['offset'] if body['hasNextPage'] else None
    }

def get_transactions(filing_nid, offset=0) -> tuple[pd.DataFrame, dict]:
    """ Get a page of transactions
        for a filingNid
        Return fields required by Socrata
    """
    tran_cols = [
        'filing_nid',
        'contributor_name',
        'contributor_type',
        'contributor_address',
        'contributor_location',
        'amount'
    ]

    params = {
        **PARAMS,
        'filingNid': filing_nid,
        'parts': 'All'
    }

    if offset > 0:
        params['offset'] = offset

    res = session.get(f'{BASE_URL}/cal/v101/transaction-elements', params=params, auth=AUTH, timeout=TIMEOUT)
    body = res.json()

    return pd.DataFrame([
        {
            'filing_nid': filing_nid,
            'contributor_name': result['allNames'],
            'contributor_type': result['transaction']['entityCd'],
            'contributor_address': ' '.join([
                str(result['addresses'][0].get('line1', '')),
                str(result['addresses'][0].get('line2', '')),
                str(result['addresses'][0].get('city', '')),
                str(result['addresses'][0].get('state', '')),
                str(result['addresses'][0].get('zip', ''))
            ]).strip() if len(result['addresses']) > 0 else '',
            'contributor_location': (
                result['addresses'][0]['latitude'], result["addresses"][0]['longitude']
             ) if len(result['addresses']) > 0 else tuple(),
             'amount': result['calculatedAmount']
        }
        for result in body['results']
    ], columns=tran_cols), {
        'page_number': body['pageNumber'],
        'has_next_page': body['hasNextPage'],
        'total': body['totalCount'],
        'count': body['count'],
        'offset': body['offset'],
        'next_offset': body['count'] + body['offset'] if body['hasNextPage'] else None
    }

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
    filing_df, response_meta = get_filings()
    print(response_meta['total'])
    print(filing_df.head())

    next_offset = response_meta['next_offset']
    end = ''
    while next_offset is not None:
        filings, filing_meta = get_filings(offset=next_offset)
        next_offset = filing_meta['next_offset']
        filing_df = pd.concat([ filing_df, filings ])
        print('¡', end=end, flush=True)

    # Keep only filings for 2020
    # This is bogus because 2020 election filings may have been received in 2019
    # Get the filer_id => election_date mapping from Suzanne
    filing_df['receipt_date'] = pd.to_datetime(filing_df['receipt_date'])
    filing_df = filing_df[filing_df['receipt_date'].dt.year == 2020]

    filing_nids = set(filing_df['filing_nid'])
    tran_df = pd.DataFrame()
    for filing_nid in filing_nids:
        transactions, response_meta = get_transactions(filing_nid)
        print(response_meta['total'])
        print(transactions.head(3))
        prog_char = '¡' if len(transactions.index) > 0 else '.'
        tran_df = pd.concat([ tran_df, transactions ])
        print(prog_char, end=end, flush=True)

        next_offset = response_meta['next_offset']
        # Get all pages of transactions
        while next_offset is not None:
            transactions, response_meta = get_transactions(filing_nid, offset=next_offset)
            next_offset = response_meta['next_offset']
            prog_char = '¡' if len(transactions.index) > 0 else '.'
            tran_df = pd.concat([ tran_df, transactions ])
            print(prog_char, end=end, flush=True)

    print('')

    df = filing_df.merge(tran_df, on='filing_nid')
    df = df.astype({
        'filer_name': 'string',
        'contributor_name': 'string',
        'contributor_type': 'string',
        'contributor_address': 'string',
        'amount': float
    })
    print(len(df.index), df.dtypes)
    pd.set_option('max_colwidth', 12)
    print(df.head(12))

    csv_cols = [
        'filer_name',
        'contributor_name',
        'contributor_type',
        'contributor_address',
        'amount',
        'receipt_date'
    ]
    df[csv_cols].to_csv('contribs_socrata.csv')

if __name__ == '__main__':
    main()
