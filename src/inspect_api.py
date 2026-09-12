"""Task 1.5 evidence: manual retrieval of at least two API pages."""
import requests, json
from collections import Counter

BASE = 'http://127.0.0.1:8000/api/events'

for page in (1, 2):
    r = requests.get(BASE, params={'page': page, 'per_page': 10}, timeout=10)
    r.raise_for_status()
    payload = r.json()
    print(f'--- GET {r.url}')
    print('pagination fields:', {k: payload[k] for k in ('page','per_page','total','has_more','next_page')})
    first = payload['items'][0]
    print('first item:', json.dumps(first))
    print()

# walk every page to show total pages and detect repeated event_ids (code-driven)
page, ids = 1, []
while True:
    p = requests.get(BASE, params={'page': page, 'per_page': 10}, timeout=10).json()
    ids += [i['event_id'] for i in p['items']]
    if not p['has_more']:
        break
    page = p['next_page']
print(f'walked {page} pages, {len(ids)} raw records, {len(set(ids))} distinct event_ids')
dupes = [e for e, n in Counter(ids).items() if n > 1]
print('event_ids appearing more than once:', dupes)
for e in dupes:
    r = requests.get(BASE, params={'page': 1, 'per_page': 50}, timeout=10)
versions = {}
page = 1
while True:
    p = requests.get(BASE, params={'page': page, 'per_page': 50}, timeout=10).json()
    for it in p['items']:
        versions.setdefault(it['event_id'], []).append(it['updated_at'])
    if not p['has_more']:
        break
    page = p['next_page']
for e in dupes:
    print(f'  {e}: updated_at versions = {sorted(versions[e])}')
print('\nWhy page 1 alone is incomplete: total records span multiple pages;')
print('stopping at page 1 would silently drop every record on later pages.')
