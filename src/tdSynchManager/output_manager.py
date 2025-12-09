"""
Output manager con auto-pulizia per ambienti Jupyter Notebook.

Previene il crash del notebook quando l'output diventa troppo grande.
"""

import sys
import time
from typing import Optional

try:
    from IPython.display import clear_output
    IPYTHON_AVAILABLE = True
except ImportError:
    IPYTHON_AVAILABLE = False


class AutoClearOutputManager:
    """Gestisce l'output con pulizia automatica quando supera limiti configurabili.

    Parametri
    ----------
    max_lines : int, optional
        Numero massimo di righe prima di pulire l'output (default: 100)
    max_chars : int, optional
        Numero massimo di caratteri prima di pulire l'output (default: 50000)
    keep_last_lines : int, optional
        Quante righe mantenere dopo la pulizia (default: 20)
    enabled : bool, optional
        Se False, disabilita completamente la pulizia (default: True)
    show_stats : bool, optional
        Se True, mostra statistiche quando pulisce (default: True)

    Esempi
    -------
    >>> # Uso base
    >>> output_mgr = AutoClearOutputManager(max_lines=50)
    >>> for i in range(200):
    ...     output_mgr.print(f"Processing {i}")

    >>> # Con context manager
    >>> with AutoClearOutputManager(max_lines=30) as out:
    ...     for i in range(100):
    ...         out.print(f"Task {i}")

    >>> # Disabilitato (nessuna pulizia)
    >>> output_mgr = AutoClearOutputManager(enabled=False)
    """

    def __init__(
        self,
        max_lines: int = 100,
        max_chars: int = 50000,
        keep_last_lines: int = 20,
        enabled: bool = True,
        show_stats: bool = True
    ):
        self.max_lines = max_lines
        self.max_chars = max_chars
        self.keep_last_lines = keep_last_lines
        self.enabled = enabled and IPYTHON_AVAILABLE
        self.show_stats = show_stats

        # Contatori
        self.line_count = 0
        self.char_count = 0
        self.total_clears = 0
        self.buffer = []

        # Stats
        self.start_time = time.time()
        self.last_clear_time = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        if self.enabled and self.show_stats and self.total_clears > 0:
            elapsed = time.time() - self.start_time
            print(f"\n[AutoClear] Sessione completata: {self.total_clears} pulizie in {elapsed:.1f}s")

    def _should_clear(self) -> bool:
        """Verifica se è necessario pulire l'output."""
        if not self.enabled:
            return False
        return self.line_count >= self.max_lines or self.char_count >= self.max_chars

    def _do_clear(self):
        """Esegue la pulizia dell'output."""
        clear_output(wait=True)

        # Mantieni solo le ultime N righe
        if self.buffer and self.keep_last_lines > 0:
            self.buffer = self.buffer[-self.keep_last_lines:]

            # Ristampa le righe mantenute
            for line in self.buffer:
                print(line)

            # Aggiorna contatori
            self.line_count = len(self.buffer)
            self.char_count = sum(len(line) for line in self.buffer)
        else:
            self.buffer = []
            self.line_count = 0
            self.char_count = 0

        # Stats
        self.total_clears += 1
        self.last_clear_time = time.time()

        if self.show_stats:
            elapsed = self.last_clear_time - self.start_time
            print(f"[AutoClear #{self.total_clears}] Output pulito dopo {elapsed:.1f}s "
                  f"(mantenute ultime {len(self.buffer)} righe)")

    def print(self, *args, **kwargs):
        """Print con auto-pulizia.

        Usa questa funzione al posto di print() normale per ottenere
        la pulizia automatica dell'output.
        """
        # Converti in stringa
        output = ' '.join(str(arg) for arg in args)

        # Aggiungi al buffer
        self.buffer.append(output)
        self.line_count += 1
        self.char_count += len(output)

        # Pulisci se necessario
        if self._should_clear():
            self._do_clear()
        else:
            # Stampa normalmente
            print(output, **kwargs)

    def clear(self):
        """Pulisci manualmente l'output."""
        if self.enabled:
            self._do_clear()

    def reset(self):
        """Reset completo dei contatori."""
        self.line_count = 0
        self.char_count = 0
        self.total_clears = 0
        self.buffer = []
        self.start_time = time.time()
        self.last_clear_time = None

    def get_stats(self) -> dict:
        """Restituisce statistiche correnti.

        Returns
        -------
        dict
            Dizionario con: line_count, char_count, total_clears, elapsed_time
        """
        return {
            'line_count': self.line_count,
            'char_count': self.char_count,
            'total_clears': self.total_clears,
            'elapsed_time': time.time() - self.start_time,
            'buffer_size': len(self.buffer)
        }


# Singleton globale opzionale
_global_output_manager: Optional[AutoClearOutputManager] = None


def get_global_output_manager() -> AutoClearOutputManager:
    """Ottieni l'istanza globale del manager (lazy init)."""
    global _global_output_manager
    if _global_output_manager is None:
        _global_output_manager = AutoClearOutputManager()
    return _global_output_manager


def set_global_output_manager(manager: AutoClearOutputManager):
    """Configura un manager globale personalizzato."""
    global _global_output_manager
    _global_output_manager = manager


def smart_print(*args, **kwargs):
    """Print intelligente che usa il manager globale."""
    get_global_output_manager().print(*args, **kwargs)
