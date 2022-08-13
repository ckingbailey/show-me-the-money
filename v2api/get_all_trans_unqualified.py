import json
from pathlib import Path
import requests
from .create_socrata_csv import AUTH, PARAMS, BASE_URL, session

def main():
    url = f'{BASE_URL}/cal/v101/transaction-elements'

    transactions = []
    bad_transactions = []
    bad_transaction = None
    next_offset = 0
    has_next_page = True
    while next_offset is not None:
        params = {
            **PARAMS,
            'parts': 'All',
            'limit': 1000
        }
        if has_next_page:
            params['offset'] = next_offset

        try:
            res = session.get(url, params=params, auth=AUTH)
        except requests.HTTPError as exc:
            print(exc, exc.args, exc.errno, exc.response)
            exc_body = exc.response.json()
            print(exc_body)
            params.pop('parts')
            res = session.get(url, params=params, auth=AUTH)

        body = res.json()
        if len(transactions) == 0:
            print('Total', body['totalCount'])

        transactions += body['results']
        print('▰', end='', flush=True)
        next_offset = body['offset'] + body['limit'] if body['hasNextPage'] is True else None

    Path('example/unfiltered_transactions.json').write_text(
        json.dumps(transactions), encoding='utf8')

if __name__ == '__main__':
    main()
