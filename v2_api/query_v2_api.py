""" Get stuff out of Netfile v2 API
"""
from pprint import PrettyPrinter
from pathlib import Path
import requests

BASE_URL = 'https://netfile.com/api/campaign'
CONTRIBUTION_FORM = 'F460A'
EXPENDITURE_FORM = 'F460E'

PARAMS = { 'aid': 'COAK' }
env_vars = {
    ln.split('=')[0]: ln.split('=')[1]
    for ln
    in  Path('.env').read_text(encoding='utf8').strip().split('\n')
}
auth = (env_vars['api_key'], env_vars['api_secret'])

pp = PrettyPrinter()

def get_filing(offset=0):
    """ Get a filing
    """
    url = f'{BASE_URL}/filing/v101/filings'

    params = { **PARAMS }
    if offset > 0:
        params['offset'] = offset

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
        **PARAMS
    }, auth=auth)
    body = res.json()

    return body['results']

def list_elections():
    """ Get all the elections
    """
    url = f'{BASE_URL}/election/v101/elections'

    res = requests.get(url, params=PARAMS, auth=auth)
    body = res.json()

    return body['results']

def get_filer(filer_nid):
    """ Get one filer
    """
    url = f'{BASE_URL}/filer/v101/filers'

    res = requests.get(url, params={ **PARAMS, 'filerNid': filer_nid }, auth=auth)
    body = res.json()

    return body['results']

if __name__ == '__main__':
    filings = get_filing()
    print('----- FILING -----')
    pp.pprint(filings[0])

    query_count = 0
    results_len = []
    transactions = []
    for i, f in enumerate(filings):
        next_trans = get_transaction(f)
        results_len.append(len(next_trans))
        transactions += next_trans
        query_count += 1
        if len(next_trans) > 0:
            tran_filing = i
            break

    print('----- TRANSACTION -----')
    print('--------- Contribution -----')
    one_contrib = [ t for t in transactions if t['calTransactionType'] == CONTRIBUTION_FORM ][0]
    pp.pprint(one_contrib)
    expenditures = [ t for t in transactions if t['calTransactionType'] == EXPENDITURE_FORM ]

    if len(expenditures) < 1:
        remaining_filings = filings[tran_filing + 1:]
        for i, f in enumerate(remaining_filings):
            next_trans = get_transaction(f)
            results_len.append(len(next_trans))
            transactions += next_trans
            query_count += 1
            expenditures = [ t for t in next_trans if t['calTransactionType'] == EXPENDITURE_FORM ]

    if len(expenditures) > 0:
        tran_filing = tran_filing + i
        print('--------- Expenditure -----')
        pp.pprint(expenditures[0])

    print('----- ALL FORM TYPES -----')
    pp.pprint(set(t['calTransactionType'] for t in transactions))

    print('----- ARE THERE ANY tranDscr AT ALL? -----')
    pp.pprint(set(t['transaction']['tranDscr'] for t in transactions))

    print('----- METRICS -----')
    print('Total number of transaction queries', len(results_len))
    print('Total number of transactions', sum(results_len))
    print('Avg size of transaction results', sum(results_len) / len(results_len))

    # filer = get_filer(one_contrib['filerNid'])
    # print('----- FILER -----')
    # pp.pprint(filer)

    # elections = list_elections()
    # pp.pprint(elections[0])

    # election_dates = [ e['electionDate'] for e in elections ]
    # print('----- ELECTION DATES -----')
    # pp.pprint(election_dates)
