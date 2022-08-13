import json
from pathlib import Path
from .create_socrata_csv import AUTH, PARAMS, BASE_URL, session

def main():
    url = f'{BASE_URL}/cal/v101/transaction-elements'
    params = {
        **PARAMS
    }

    transactions = []
    next_offset = True
    while next_offset is not None:
        if type(next_offset) == int:
            params['offset'] = next_offset

        res = session.get(url, params=params, auth=AUTH)
        body = res.json()
        if len(transactions) == 0:
            print('Total', body['totalCount'])

        transactions += body['results']
        print('▰', end='', flush=True)
        next_offset = body['offset'] + body['limit'] if body['hasNextPage'] is True else None

    Path('example/unfiltered_transactions.json').write_text(json.dumps(transactions), encoding='utf8')

if __name__ == '__main__':
    main()
