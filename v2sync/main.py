import argparse
import json
import os
from pathlib import Path
from pprint import PrettyPrinter
import requests
from v2api.create_socrata_csv import (
    session as rsession,
    BASE_URL,
    PARAMS,
    AUTH)

SUBSCRIPTION_ID = os.environ['SUBSCRIPTION_ID']
pp = PrettyPrinter()

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
    print('Subscribed', flush=True)
    pp.pprint(body)

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

    print('Subscription peek', flush=True)
    pp.pprint(body)
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

    print('Session started', flush=True)
    pp.pprint(body)
    session_id = body['session']['id']
    return {
        'session_id': session_id,
        'endpoints': {
            t['topicName']: f'{BASE_URL}/filing/v101/sync/sessions/{session_id}/{t["topicName"]}'
            for t in body['topicLinks']
        }
    }

def sync_all_feeds(session):
    """ Pull latest data from specified URLs
    """
    data = {}
    for k, url in session['endpoints'].items():
        res = rsession.get(url, params=PARAMS, auth=AUTH)
        body = res.json()
        print(f'Made request to {res.url}. Got response {res}.', flush=True)

        data[k] = body['results']

    return data

def save_data(data: dict[list]):
    """ Save downloaded data to JSON files """
    total_chars_written = {}
    for k, v in data.items():
        outpath = (Path(__file__).parent / f'data/{k}.json')
        chars_written = outpath.write_text(
            json.dumps(v, indent=4),
            encoding='utf8')
        print(f'Wrote {chars_written} to {outpath.resolve()}', flush=True)
        total_chars_written[k] = chars_written

    return total_chars_written

def close_session(sub_id, session_id):
    """ End NetFile sync session
    """
    url = f'{BASE_URL}/sync/v101/sessions/{session_id}/commands/complete'
    req_body = {
        'subscriptionId': sub_id
    }

    res = rsession.post(url, params=PARAMS, auth=AUTH, json=req_body)
    body = res.json()

    print('Close session', flush=True)
    pp.pprint(body)
    return body

def sync_latest_data():
    """ Peek subscription
        Start session
        Download latest data
        Save latest data to disc
        Close session
    """
    new_data = {}
    try:
        peek = peek_subscription(SUBSCRIPTION_ID)
        print('Peek:', peek)
        if peek is True:
            session = start_session(SUBSCRIPTION_ID)

            new_data = sync_all_feeds(session)
            print('Retrieved new data', {
                k: len(v)
                for k, v in new_data.items()
            })
            saved = save_data(new_data)

    except requests.exceptions.HTTPError as exc:
        req = exc.request
        print('Error:', exc.response)
        print('headers:', req.headers)
        print('body:', req.body)
        print('url:', req.url)
        raise
    finally:
        close_session(SUBSCRIPTION_ID, session['session_id'])

    return {
        'downloaded': sum(len(v) for v in new_data.values()),
        'saved': sum(v for v in saved.values())
    }

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
