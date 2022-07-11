""" Get stuff out of Netfile v2 API
"""
from pprint import PrettyPrinter
from pathlib import Path
import requests

BASE_URL = 'https://netfile.com/api/campaign'

params = { 'aid': 'COAK' }
env_vars = {
    ln.split('=')[0]: ln.split('=')[1]
    for ln
    in  Path('.env').read_text(encoding='utf8').strip().split('\n')
}
auth = (env_vars['api_key'], env_vars['api_secret'])

pp = PrettyPrinter()

def get_filing():
    """ Get a filing
    """
    url = f'{BASE_URL}/filing/v101/filings'

    res = requests.get(url, params=params, auth=auth)
    body = res.json()
    
    return body['results']

def get_transaction(filing):
    """ Get a transaction
    """
    url = f'{BASE_URL}/cal/v101/transaction-elements'

    res = requests.get(url, params={
        'filingNid': filing['filingNid'],
        'parts': 'All',
        **params
    }, auth=auth)
    body = res.json()

    return body['results']

def list_elections():
    """ Get all the elections
    """
    url = f'{BASE_URL}/election/v101/elections'

    res = requests.get(url, params=params, auth=auth)
    body = res.json()

    return body['results']

def get_filer(filer_nid):
    """ Get one filer
    """
    url = f'{BASE_URL}/filer/v101/filers'

    res = requests.get(url, params={ **params, 'filerNid': filer_nid }, auth=auth)
    body = res.json()

    return body['results']

if __name__ == '__main__':
    filings = get_filing()
    print('----- FILING -----')
    pp.pprint(filings[0])

    for f in filings:
        transactions = get_transaction(f)
        if len(transactions) > 0:
            break

    # pp.pprint(transactions[0])
    one_transaction = [ t for t in transactions if t['calTransactionType'] == 'F460A' ][0]
    print('----- TRANSACTION -----')
    pp.pprint(one_transaction)

    address_types = {
        a['addressType'] for t in transactions for a in t['addresses']
    }
    print('---- ADDRESS TYPES -----')
    pp.pprint(address_types)

    filer = get_filer(one_transaction['filerNid'])
    print('----- FILER -----')
    pp.pprint(filer)

    elections = list_elections()
    pp.pprint(elections[0])

    election_dates = [ e['electionDate'] for e in elections ]
    print('----- ELECTION DATES -----')
    pp.pprint(election_dates)

