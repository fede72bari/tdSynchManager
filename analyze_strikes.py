import pandas as pd

# Read files
local_df = pd.read_csv('local_contracts_20220202.csv')
api_df = pd.read_csv('api_eod_20220202.csv')

# Focus on 2022-02-02 expiration (same day)
exp = '2022-02-02'

# Get strikes
local_strikes = sorted(local_df[local_df['expiration'] == exp]['strike'].unique())
api_strikes = sorted(api_df[api_df['expiration'] == exp]['strike'].unique())

print(f'Expiration: {exp}')
print(f'API strikes: {len(api_strikes)} - range [{min(api_strikes):.0f} - {max(api_strikes):.0f}]')
print(f'Local strikes: {len(local_strikes)} - range [{min(local_strikes):.0f} - {max(local_strikes):.0f}]')

missing_strikes = sorted(set(api_strikes) - set(local_strikes))
print(f'\nMissing: {len(missing_strikes)} strikes')
if missing_strikes:
    print(f'Missing range: [{min(missing_strikes):.0f} - {max(missing_strikes):.0f}]')

# SPY price on 2022-02-02 was around 450
spy_price_approx = 450

print(f'\nAssuming SPY price ~{spy_price_approx}:')
print(f'  Lowest API strike: {min(api_strikes):.0f} (OTM: ${abs(spy_price_approx - min(api_strikes)):.0f})')
print(f'  Highest API strike: {max(api_strikes):.0f} (OTM: ${abs(max(api_strikes) - spy_price_approx):.0f})')
print(f'  Lowest LOCAL strike: {min(local_strikes):.0f} (OTM: ${abs(spy_price_approx - min(local_strikes)):.0f})')
print(f'  Highest LOCAL strike: {max(local_strikes):.0f} (OTM: ${abs(max(local_strikes) - spy_price_approx):.0f})')

print(f'\nMissing strikes (first 20 of {len(missing_strikes)}):')
for s in missing_strikes[:20]:
    otm = abs(s - spy_price_approx)
    side = 'OTM' if (s < spy_price_approx - 20 or s > spy_price_approx + 20) else 'ATM'
    print(f'  {s:6.0f} ({side}, distance: ${otm:.0f})')

if len(missing_strikes) > 20:
    print(f'\nMissing strikes (last 10):')
    for s in missing_strikes[-10:]:
        otm = abs(s - spy_price_approx)
        side = 'OTM' if (s < spy_price_approx - 20 or s > spy_price_approx + 20) else 'ATM'
        print(f'  {s:6.0f} ({side}, distance: ${otm:.0f})')
