from pathlib import Path
import requests
from v2api.create_socrata_csv import (
    session as rsession,
    BASE_URL,
    PARAMS,
    AUTH)

subscription_id = [
    ln.split('=')[1] for ln in Path('.env').read_text(encoding='utf8').strip().split('\n')
    if ln.startswith('subscription_id=')
][0]

def create_subscription():
    """ Begin subscription to NetFile sync API
    """
    endpoint = '/filing/v101/sync/subscribe'
    body = {
        'name': 'ckb_test_subscription',
        'filter': {
            'aid': PARAMS['aid'],
            'topics': [
                'filing-activities',
                'element-activities'
            ]
        }
    }
    headers = {
        'content-type': 'application/json'
    }
    print(body)

    try:
        res = rsession.post(f'{BASE_URL}{endpoint}',
        params=PARAMS,
        headers=headers,
        json=body,
        auth=AUTH)
    except requests.exceptions.HTTPError as exc:
        req = exc.request
        print(req.headers)
        print(req.body)
        print(req.url)
        raise

    body = res.json()

    print(body.keys(), body, sep='\n')
    subscription_id = body['id']
    owner = body['owner']

    return owner, subscription_id

def peek_subscription(sub_id):
    """ Peek subscription to see if there's new stuff
    """
    endpoint = f'{BASE_URL}/sync/v101/subscriptions/{sub_id}/peek'

    res = rsession.get(endpoint, params=PARAMS, auth=AUTH)
    body = res.json()
    
    return body

if __name__ == '__main__':
    print(peek_subscription(subscription_id))
