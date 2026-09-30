"""Mimics Nse.get_symbols + Nse.get_data flow from NSE_Option_Chain_Analyzer.py to verify realtime fetch."""
import json
import sys
import requests

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/130.0.0.0 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate'}

session = requests.Session()

# Step 1: same as get_symbols() - homepage for cookies
r = session.get('https://www.nseindia.com/option-chain', headers=headers, timeout=5)
cookies = dict(r.cookies)
print(f'STEP1 homepage status={r.status_code} cookies={list(cookies.keys())}')

# Step 2: underlying-info (indices + stocks lists)
r2 = session.get('https://www.nseindia.com/api/underlying-information', headers=headers, timeout=5, cookies=cookies)
j2 = r2.json()
indices = [item['symbol'] for item in j2['data']['IndexList']]
print(f'STEP2 symbols status={r2.status_code} n_indices={len(indices)} first3={indices[:3]}')

# Step 3: option chain for NIFTY nearest expiry (same API the app polls on refresh)
r3 = session.get('https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY',
                 headers=headers, timeout=5, cookies=cookies)
j3 = r3.json()
expiry = j3['expiryDates'][0]
print(f'STEP3 expiries status={r3.status_code} nearest_expiry={expiry}')

r4 = session.get(f'https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={expiry}',
                 headers=headers, timeout=5, cookies=cookies)
j4 = r4.json()
recs = j4['records']
print(f'STEP4 chain status={r4.status_code}')
print(f'  server_timestamp={recs.get("timestamp")}')
print(f'  underlyingValue={recs.get("underlyingValue")}')
data = recs['data']
tot_oi = sum(d['openInterest'] for d in data if d.get('openInterest'))
print(f'  strikes={len(data)} total_open_interest={tot_oi}')
print(f'  local_time_when_fetched={__import__("datetime").datetime.now():%Y-%m-%d %H:%M:%S}')
print('PROBE_OK: realtime fetch works')
