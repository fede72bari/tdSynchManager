"""
Retry logic con exponential backoff per InfluxDB writes
Gestisce timeout e salva batch falliti per recovery
"""

import time
import os
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime


class InfluxWriteRetry:
    """Gestisce retry e recovery per scritture InfluxDB fallite."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 5.0,
        max_delay: float = 60.0,
        failed_batch_dir: Optional[str] = None
    ):
        """
        Parametri
        ----------
        max_retries : int
            Numero massimo di retry per batch
        base_delay : float
            Delay iniziale in secondi (poi exponential backoff)
        max_delay : float
            Delay massimo tra retry
        failed_batch_dir : str, optional
            Directory dove salvare batch falliti per recovery manuale
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.failed_batch_dir = failed_batch_dir

        if failed_batch_dir:
            Path(failed_batch_dir).mkdir(parents=True, exist_ok=True)

        # Statistics
        self.stats = {
            'total_batches': 0,
            'successful': 0,
            'failed': 0,
            'retried': 0,
            'recovered_on_retry': 0
        }

    def write_with_retry(
        self,
        client,
        lines: List[str],
        measurement: str,
        batch_idx: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Scrive batch con retry automatico.

        Returns
        -------
        bool
            True se scrittura riuscita, False se fallita dopo tutti i retry
        """
        self.stats['total_batches'] += 1

        for attempt in range(self.max_retries):
            try:
                # Tentativo di scrittura
                ret = client.write(record=lines)

                # Success
                self.stats['successful'] += 1
                if attempt > 0:
                    self.stats['recovered_on_retry'] += 1
                    print(f"[INFLUX][RETRY-SUCCESS] measurement={measurement} "
                          f"batch={batch_idx} recovered after {attempt} retries")

                return True

            except Exception as e:
                error_type = type(e).__name__
                is_timeout = 'timeout' in str(e).lower() or 'timed out' in str(e).lower()
                is_auth_error = 'unauthorized' in str(e).lower() or '401' in str(e) or '403' in str(e)

                # Auth errors: non retry, rilancia subito
                if is_auth_error:
                    print(f"[INFLUX][FATAL] Auth error - no retry: {e}")
                    raise

                # Timeout o errore generico
                if attempt < self.max_retries - 1:
                    # Calcola delay con exponential backoff
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)

                    self.stats['retried'] += 1
                    print(f"[INFLUX][RETRY] measurement={measurement} batch={batch_idx} "
                          f"attempt={attempt+1}/{self.max_retries} error={error_type} "
                          f"waiting {delay:.1f}s before retry...")

                    time.sleep(delay)
                else:
                    # Fallito dopo tutti i retry
                    self.stats['failed'] += 1
                    print(f"[INFLUX][FAILED] measurement={measurement} batch={batch_idx} "
                          f"failed after {self.max_retries} attempts: {error_type}: {e}")

                    # Salva batch fallito per recovery manuale
                    if self.failed_batch_dir:
                        self._save_failed_batch(
                            lines=lines,
                            measurement=measurement,
                            batch_idx=batch_idx,
                            error=str(e),
                            metadata=metadata
                        )

                    return False

        return False

    def _save_failed_batch(
        self,
        lines: List[str],
        measurement: str,
        batch_idx: int,
        error: str,
        metadata: Optional[Dict[str, Any]]
    ):
        """Salva batch fallito su disco per recovery manuale."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"failed_{measurement}_batch{batch_idx}_{timestamp}.json"
        filepath = os.path.join(self.failed_batch_dir, filename)

        failed_batch = {
            'measurement': measurement,
            'batch_idx': batch_idx,
            'timestamp': timestamp,
            'error': error,
            'num_lines': len(lines),
            'lines': lines,
            'metadata': metadata or {}
        }

        try:
            with open(filepath, 'w') as f:
                json.dump(failed_batch, f, indent=2)
            print(f"[INFLUX][SAVED-FAILED] Batch salvato in: {filepath}")
        except Exception as e:
            print(f"[INFLUX][ERROR] Impossibile salvare batch fallito: {e}")

    def get_stats(self) -> Dict[str, int]:
        """Restituisce statistiche retry."""
        return self.stats.copy()

    def print_summary(self):
        """Stampa summary delle statistiche."""
        s = self.stats
        total = s['total_batches']
        if total == 0:
            return

        success_rate = 100 * s['successful'] / total
        retry_rate = 100 * s['retried'] / total
        recovery_rate = 100 * s['recovered_on_retry'] / total if s['retried'] > 0 else 0

        print("\n" + "=" * 80)
        print("INFLUX WRITE SUMMARY")
        print("=" * 80)
        print(f"Total batches:          {total}")
        print(f"Successful (1st try):   {s['successful'] - s['recovered_on_retry']}")
        print(f"Retried:                {s['retried']}")
        print(f"Recovered on retry:     {s['recovered_on_retry']}")
        print(f"Failed (permanent):     {s['failed']}")
        print(f"Success rate:           {success_rate:.1f}%")
        print(f"Retry rate:             {retry_rate:.1f}%")
        if s['retried'] > 0:
            print(f"Recovery rate:          {recovery_rate:.1f}%")
        print("=" * 80)


def recover_failed_batches(failed_batch_dir: str, client, dry_run: bool = False):
    """
    Recupera batch falliti da disco.

    Parametri
    ----------
    failed_batch_dir : str
        Directory contenente i file JSON dei batch falliti
    client : InfluxDBClient3
        Client InfluxDB per scrivere
    dry_run : bool
        Se True, mostra solo cosa farebbe senza scrivere
    """
    print("=" * 80)
    print(f"RECOVERING FAILED BATCHES from {failed_batch_dir}")
    print("=" * 80)

    failed_files = list(Path(failed_batch_dir).glob("failed_*.json"))

    if not failed_files:
        print("[RECOVERY] No failed batches found")
        return

    print(f"[RECOVERY] Found {len(failed_files)} failed batches")

    recovered = 0
    still_failed = 0

    for filepath in sorted(failed_files):
        try:
            with open(filepath, 'r') as f:
                batch_data = json.load(f)

            measurement = batch_data['measurement']
            lines = batch_data['lines']
            batch_idx = batch_data['batch_idx']
            original_error = batch_data.get('error', 'unknown')

            print(f"\n[RECOVERY] Processing {filepath.name}")
            print(f"[RECOVERY]   Measurement: {measurement}")
            print(f"[RECOVERY]   Lines: {len(lines)}")
            print(f"[RECOVERY]   Original error: {original_error[:100]}...")

            if dry_run:
                print(f"[RECOVERY]   DRY-RUN: Would attempt to write {len(lines)} lines")
                continue

            # Tentativo di recovery
            try:
                client.write(record=lines)
                print(f"[RECOVERY]   ✓ SUCCESS - Batch recovered!")

                # Rinomina file per marcarlo come recuperato
                recovered_path = filepath.with_suffix('.recovered')
                filepath.rename(recovered_path)
                recovered += 1

            except Exception as e:
                print(f"[RECOVERY]   ✗ STILL FAILING: {type(e).__name__}: {e}")
                still_failed += 1

        except Exception as e:
            print(f"[RECOVERY] Error processing {filepath}: {e}")
            still_failed += 1

    print("\n" + "=" * 80)
    print("RECOVERY SUMMARY")
    print("=" * 80)
    print(f"Total files:      {len(failed_files)}")
    print(f"Recovered:        {recovered}")
    print(f"Still failing:    {still_failed}")
    print(f"Skipped (dry):    {len(failed_files) - recovered - still_failed if dry_run else 0}")
    print("=" * 80)
