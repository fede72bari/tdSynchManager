from console_log import log_console
import re
from datetime import datetime
from collections import defaultdict
import statistics

# Read the log file
with open('log options 5m.txt', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Track detailed events per day
day_data = defaultdict(lambda: {
    'timestamps': [],
    'greek_calls': [],
    'influx_writes': [],
    'total_rows': 0,
    'symbols': set(),
    'events': []  # Track all events with timestamps
})

for line in lines:
    # Extract timestamp
    ts_match = re.match(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]', line)
    if not ts_match:
        continue
    timestamp = datetime.strptime(ts_match.group(1), '%Y-%m-%d %H:%M:%S.%f')

    # RESUME-INFLUX CHECK marks day start
    if 'RESUME-INFLUX][CHECK]' in line:
        match = re.search(r'range=\[(\d{4}-\d{2}-\d{2})', line)
        symbol_match = re.search(r'meas=([A-Z]+)-', line)
        if match:
            date = match.group(1).replace('-', '')
            day_data[date]['timestamps'].append(timestamp)
            day_data[date]['events'].append(('START', timestamp, line.strip()))
            if symbol_match:
                day_data[date]['symbols'].add(symbol_match.group(1))

    # Greek API calls
    if 'option/history/greeks/' in line:
        match = re.search(r'date=(\d{8})', line)
        symbol_match = re.search(r'symbol=([A-Z]+)', line)
        time_match = re.search(r'->\s*200 in ([\d.]+) ms', line)
        if match and time_match:
            date = match.group(1)
            time_ms = float(time_match.group(1))
            day_data[date]['timestamps'].append(timestamp)
            day_data[date]['greek_calls'].append(time_ms)
            day_data[date]['events'].append(('GREEK', timestamp, time_ms, line.strip()))
            if symbol_match:
                day_data[date]['symbols'].add(symbol_match.group(1))

    # InfluxDB ABOUT-TO-WRITE shows total rows
    if '[INFLUX][ABOUT-TO-WRITE]' in line:
        match = re.search(r'window_ET=\((\d{4}-\d{2}-\d{2})', line)
        rows_match = re.search(r'rows=(\d+)', line)
        if match and rows_match:
            date = match.group(1).replace('-', '')
            day_data[date]['total_rows'] += int(rows_match.group(1))
            day_data[date]['timestamps'].append(timestamp)
            day_data[date]['events'].append(('INFLUX_START', timestamp, line.strip()))

    # InfluxDB WRITE batches
    if '[INFLUX][WRITE]' in line:
        symbol_match = re.search(r'measurement=([A-Z]+)-', line)
        batch_match = re.search(r'batch=(\d+)/(\d+)', line)
        if symbol_match and batch_match:
            for date in sorted(day_data.keys(), reverse=True):
                if symbol_match.group(1) in day_data[date]['symbols']:
                    day_data[date]['timestamps'].append(timestamp)
                    day_data[date]['influx_writes'].append(1)
                    if batch_match.group(1) == batch_match.group(2):  # Last batch
                        day_data[date]['events'].append(('INFLUX_END', timestamp, line.strip()))
                    break

# Calculate statistics for each day
day_stats = []
for date in sorted(day_data.keys()):
    data = day_data[date]
    if not data['timestamps'] or len(data['greek_calls']) == 0:
        continue

    start_time = min(data['timestamps'])
    end_time = max(data['timestamps'])
    duration_sec = (end_time - start_time).total_seconds()

    greek_total_ms = sum(data['greek_calls'])
    greek_slow = len([t for t in data['greek_calls'] if t > 10000])

    day_stats.append({
        'date': date,
        'symbols': list(data['symbols']),
        'start_time': start_time,
        'end_time': end_time,
        'duration_sec': duration_sec,
        'greek_calls': len(data['greek_calls']),
        'greek_total_ms': greek_total_ms,
        'greek_slow': greek_slow,
        'greek_max': max(data['greek_calls']),
        'influx_batches': len(data['influx_writes']),
        'total_rows': data['total_rows'],
        'events': data['events']
    })

# Sort by duration
day_stats_sorted = sorted(day_stats, key=lambda x: x['duration_sec'], reverse=True)

log_console('=' * 80)
log_console('ANALISI CASI PEGGIORI - GIORNATE PIÙ LENTE')
log_console('=' * 80)
log_console()

# Distribuzione per tempo
durations = [d['duration_sec'] for d in day_stats]
count_30s = len([d for d in durations if d > 30])
count_120s = len([d for d in durations if d > 120])
count_300s = len([d for d in durations if d > 300])
count_600s = len([d for d in durations if d > 600])
count_1800s = len([d for d in durations if d > 1800])

log_console('DISTRIBUZIONE DURATE:')
log_console('  Totale giorni: {}'.format(len(day_stats)))
log_console('  >30s (0.5 min): {} giorni ({:.1f}%)'.format(count_30s, 100*count_30s/len(day_stats)))
log_console('  >120s (2 min): {} giorni ({:.1f}%)'.format(count_120s, 100*count_120s/len(day_stats)))
log_console('  >300s (5 min): {} giorni ({:.1f}%)'.format(count_300s, 100*count_300s/len(day_stats)))
log_console('  >600s (10 min): {} giorni ({:.1f}%)'.format(count_600s, 100*count_600s/len(day_stats)))
log_console('  >1800s (30 min): {} giorni ({:.1f}%)'.format(count_1800s, 100*count_1800s/len(day_stats)))
log_console()

# Top 10 giorni peggiori
log_console('=' * 80)
log_console('TOP 10 GIORNI PIÙ LENTI:')
log_console('=' * 80)
header = '{:<10} {:<10} {:<19} {:>10} {:>8} {:>8} {:>7}'.format(
    'Date', 'Symbol', 'Start Time', 'Duration', 'Greeks', 'Slow', 'Rows')
log_console(header)
log_console('-' * 80)

for i, d in enumerate(day_stats_sorted[:10]):
    symbols_str = ','.join(sorted(d['symbols']))[:10]
    start_str = d['start_time'].strftime('%H:%M:%S')
    dur_min = d['duration_sec'] / 60
    row = '{} {:<10} {} {:8.1f}m {:6.1f}s {:3d} {:6d}'.format(
        d['date'], symbols_str, start_str, dur_min,
        d['greek_total_ms']/1000, d['greek_slow'], d['total_rows']
    )
    log_console(row)
log_console()

# Analisi dettagliata del caso peggiore
worst = day_stats_sorted[0]
log_console('=' * 80)
log_console('DETTAGLIO CASO PEGGIORE: {} (durata: {:.1f} minuti)'.format(
    worst['date'], worst['duration_sec']/60))
log_console('=' * 80)
log_console('Simbolo: {}'.format(','.join(worst['symbols'])))
log_console('Inizio: {}'.format(worst['start_time']))
log_console('Fine: {}'.format(worst['end_time']))
log_console('Durata: {:.1f} secondi ({:.1f} minuti)'.format(worst['duration_sec'], worst['duration_sec']/60))
log_console('Greek calls: {} (totale: {:.1f}s, slow: {})'.format(
    worst['greek_calls'], worst['greek_total_ms']/1000, worst['greek_slow']))
log_console('InfluxDB batches: {}'.format(worst['influx_batches']))
log_console('Righe: {}'.format(worst['total_rows']))
log_console()

# Trova la pausa più lunga tra eventi
events = worst['events']
max_gap = 0
max_gap_info = None
for i in range(1, len(events)):
    gap = (events[i][1] - events[i-1][1]).total_seconds()
    if gap > max_gap:
        max_gap = gap
        max_gap_info = (events[i-1], events[i])

if max_gap_info:
    log_console('PAUSA PIÙ LUNGA: {:.1f} secondi ({:.1f} minuti)'.format(max_gap, max_gap/60))
    log_console('  Tra: {} -> {}'.format(
        max_gap_info[0][1].strftime('%H:%M:%S'),
        max_gap_info[1][1].strftime('%H:%M:%S')))
    log_console('  Evento prima: {}'.format(max_gap_info[0][0]))
    log_console('  Evento dopo: {}'.format(max_gap_info[1][0]))
log_console()

# Breakdown tempo per questo giorno
greek_time = worst['greek_total_ms'] / 1000
other_time = worst['duration_sec'] - greek_time
log_console('BREAKDOWN TEMPO:')
log_console('  Greek API: {:.1f}s ({:.1f}%)'.format(greek_time, 100*greek_time/worst['duration_sec']))
log_console('  Altro (InfluxDB + processing): {:.1f}s ({:.1f}%)'.format(
    other_time, 100*other_time/worst['duration_sec']))
log_console()

# Analizza top 5 peggiori
log_console('=' * 80)
log_console('BREAKDOWN TEMPO TOP 5 GIORNI PEGGIORI:')
log_console('=' * 80)
header = '{:<10} {:>10} {:>10} {:>8} {:>8}'.format('Date', 'Duration', 'Greeks', 'InfluxDB', 'Other')
log_console(header)
log_console('-' * 80)
for d in day_stats_sorted[:5]:
    greek_time = d['greek_total_ms'] / 1000
    influx_time = d['influx_batches'] * 1.0  # Rough estimate: 1s per batch
    other_time = d['duration_sec'] - greek_time - influx_time
    log_console('{} {:8.1f}m {:8.1f}s {:6.1f}s {:6.1f}s'.format(
        d['date'], d['duration_sec']/60, greek_time, influx_time, other_time
    ))
