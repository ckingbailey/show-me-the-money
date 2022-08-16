import argparse
import json
from pathlib import Path
import requests
from v2api.create_socrata_csv import (
    session as rsession,
    BASE_URL,
    PARAMS,
    AUTH)

SUBSCRIPTION_ID = [
    ln.split('=')[1] for ln in Path('.env').read_text(encoding='utf8').strip().split('\n')
    if ln.startswith('subscription_id=')
][0]

def get_subscription_options():
    """ See what feeds we can subscribe to
    """
    url = f'{BASE_URL}/sync/v101/feeds/filing_v101'

    res = rsession.get(url, params=PARAMS, auth=AUTH)
    body = res.json()

    return body

def create_subscription(sub_name):
    """ Begin subscription to NetFile sync API
    """
    endpoint = '/filing/v101/sync/subscribe'
    body = {
        'name': sub_name,
        'filter': {
            'aid': PARAMS['aid'],
            'topics': [
                'filings',
                'transaction-elements'
            ]
        }
    }
    headers = {
        'content-type': 'application/json'
    }
    print(body)

    res = rsession.post(f'{BASE_URL}{endpoint}',
    params=PARAMS,
    headers=headers,
    json=body,
    auth=AUTH)

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

    print(body)
    return body['dataAvailable']

def start_session(sub_id):
    """ Start a session to pull new data
    """
    endpoint = F'{BASE_URL}/sync/v101/sessions'
    req_body = {
        "subscriptionId": sub_id,
        "sequenceRangeBegin": 0 # provided to offset into the dataset so our samples have element activity (early filings are all paper)
    }

    res = rsession.post(endpoint, params=PARAMS, auth=AUTH, json=req_body)
    body = res.json()

    print(body)
    return body['session']['id']

def sync_all_feeds(session_id):
    """ Pull latest data from specified URLs
    """
    endpoints = {
        'filing_activity': f'{BASE_URL}/filing/v101/sync/sessions/{session_id}/filing-activities',
        'element_activity': f'{BASE_URL}/filing/v101/sync/sessions/{session_id}/element-activities'
    }

    data = { k: '' for k in endpoints.keys() }
    for k, url in endpoints.items():
        res = rsession.get(url, params=PARAMS, auth=AUTH)
        body = res.json()

        data[k] = body['results']

    return data

def save_data(data: dict[list]):
    """ Save downloaded data to JSON files """
    for k, v in data.items():
        outpath = (Path(__file__).parent / f'example/data/{k}.json')
        chars_written = outpath.write_text(
            json.dumps(v, indent=4),
            encoding='utf8')
        print(f'Wrote {chars_written} to {outpath.resolve()}')

def close_session(sub_id, session_id):
    """ End NetFile sync session
    """
    url = f'{BASE_URL}/sync/v101/sessions/{session_id}/commands/complete'
    req_body = {
        'subscriptionId': sub_id
    }

    res = rsession.post(url, params=PARAMS, auth=AUTH, json=req_body)
    body = res.json()

    print(body)
    return body

def sync_latest_data():
    """ Peek subscription
        Start session
        Download latest data
        Save latest data to disc
        Close session
    """
    try:
        peek = peek_subscription(SUBSCRIPTION_ID)
        print(peek)
        if peek is True:
            session_id = start_session(SUBSCRIPTION_ID)

            new_data = sync_all_feeds(session_id)
            print({
                k: len(v)
                for k, v in new_data.items()
            })
            save_data(new_data)

    except requests.exceptions.HTTPError as exc:
        req = exc.request
        print(req.headers)
        print(req.body)
        print(req.url)
        raise
    finally:
        close_session(SUBSCRIPTION_ID, session_id)

def main():
    """ Depending on input, do one of:
        - Print available feeds
        - Create new subscription
        - Pull latest data
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed',
        action='store_true',
        help='Display the list of feeds available to subscribe to')
    parser.add_argument('--subscribe',
        help='Create a new subscription, giving it the specified name')

    args = parser.parse_args()

    if args.feed:
        print(get_subscription_options())
    elif args.subscribe:
        print(create_subscription(args.subscribe))
    else:
        print(sync_latest_data())

if __name__ == '__main__':
    main()
