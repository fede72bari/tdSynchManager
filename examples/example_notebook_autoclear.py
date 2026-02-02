"""
Esempio: Come usare AutoClearOutputManager con ThetaSyncManager
Per evitare crash del notebook durante sincronizzazioni lunghe
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from tdSynchManager.output_manager import AutoClearOutputManager

# =============================================================================
# METODO 1: Wrapper Completo (CONSIGLIATO per notebook)
# =============================================================================

async def run_sync_with_autoclear(cfg, tasks, max_output_lines=100):
    """
    Wrapper che esegue la sincronizzazione con pulizia automatica output.

    Parametri
    ----------
    cfg : ManagerConfig
        Configurazione del manager
    tasks : List[Task]
        Lista di task da eseguire
    max_output_lines : int
        Numero massimo di righe prima di pulire l'output
    """
    # Crea il manager con auto-pulizia
    with AutoClearOutputManager(
        max_lines=max_output_lines,
        keep_last_lines=20,
        show_stats=True
    ) as output_mgr:

        # Override del print globale (solo in questo context)
        import builtins
        original_print = builtins.print
        builtins.print = output_mgr.print

        try:
            async with ThetaDataV3Client() as client:
                manager = ThetaSyncManager(cfg, client=client)
                await manager.run(tasks)
        finally:
            # Ripristina print originale
            builtins.print = original_print

        # Mostra stats finali
        stats = output_mgr.get_stats()
        log_console(f"\n{'='*80}")
        log_console(f"STATISTICHE OUTPUT:")
        log_console(f"  Pulizie eseguite: {stats['total_clears']}")
        log_console(f"  Tempo totale: {stats['elapsed_time']:.1f}s")
        log_console(f"  Righe finali: {stats['line_count']}")
        log_console(f"{'='*80}")


# =============================================================================
# METODO 2: Semplice (per uso occasionale)
# =============================================================================

async def simple_autoclear_example():
    """Esempio semplice con auto-pulizia."""
    from tdSynchManager.credentials import get_influx_credentials

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    cfg = ManagerConfig(
        root_dir=r"C:\\Users\\Federico\\Downloads",
        max_concurrency=80,
        influx_url=influx_url,
        influx_bucket=influx_bucket,
        influx_token=influx_token,
    )

    tasks = [
        Task(
            asset="option",
            symbols=["SPY", "QQQ", "AAPL", "MSFT"],
            intervals=["1d", "5m"],
            sink="influxdb",
            enrich_bar_greeks=True,
            discover_policy=DiscoverPolicy(mode="mild_skip")
        ),
    ]

    # Esegui con auto-pulizia (max 50 righe)
    await run_sync_with_autoclear(cfg, tasks, max_output_lines=50)


# =============================================================================
# METODO 3: Notebook-Friendly con Progress Updates
# =============================================================================

async def notebook_friendly_sync(cfg, tasks):
    """
    Versione ottimizzata per notebook con aggiornamenti progressivi compatti.
    """
    from IPython.display import clear_output
    import time

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)

        # Monitora progresso con auto-pulizia
        start_time = time.time()
        log_console("Sincronizzazione avviata...")

        # Esegui (questo produrrà molto output)
        await manager.run(tasks)

        # Summary finale
        clear_output(wait=True)
        elapsed = time.time() - start_time
        log_console(f"✓ Sincronizzazione completata in {elapsed:.1f}s")


# =============================================================================
# TEST
# =============================================================================

async def test_autoclear():
    from tdSynchManager.credentials import get_influx_credentials

    log_console("=" * 80)
    log_console("TEST: ThetaSyncManager con AutoClearOutputManager")
    log_console("=" * 80)

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    cfg = ManagerConfig(
        root_dir=r"C:\\Users\\Federico\\Downloads",
        max_concurrency=80,
        influx_url=influx_url,
        influx_bucket=influx_bucket,
        influx_token=influx_token,
    )

    # Task di test (piccolo)
    tasks = [
        Task(
            asset="option",
            symbols=["AAPL"],
            intervals=["1d"],
            sink="influxdb",
            enrich_bar_greeks=True,
            first_date_override="20241201",
            end_date_override="20241205",
            discover_policy=DiscoverPolicy(mode="mild_skip")
        ),
    ]

    log_console("\nEsecuzione con auto-pulizia (max 30 righe)...")
    log_console("-" * 80)

    await run_sync_with_autoclear(cfg, tasks, max_output_lines=30)

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETATO")
    log_console("=" * 80)


if __name__ == "__main__":
    # Esegui test
    asyncio.run(test_autoclear())
