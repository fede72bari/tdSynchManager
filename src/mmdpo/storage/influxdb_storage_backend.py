from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from tdSynchManager.config import ManagerConfig
from tdSynchManager.influx_retry import (
    delete_failed_batch,
    list_failed_batches,
    recover_failed_batches,
)
from tdSynchManager.manager import ThetaSyncManager


@dataclass(frozen=True)
class DatasetKeyParts:
    """Parsed dataset key components for InfluxDB storage.

    Attributes
    ----------
    provider : str
        Provider name from canonical key.
    asset : str
        Asset class (`stock`, `index`, `option`).
    symbol : str
        Symbol/root.
    interval : str
        Interval identifier.
    storage_backend : str
        Storage backend name (`influxdb` for this class).
    """

    provider: str
    asset: str
    symbol: str
    interval: str
    storage_backend: str


class _NoopThetaClient:
    """No-op client used only to initialize legacy manager composition."""


class InfluxDBStorageBackend:
    """InfluxDB backend that delegates to legacy ThetaSyncManager logic.

    This class intentionally reuses legacy methods for Influx writes/retry/recovery
    so behavior remains faithful to the existing production pipeline.
    """

    storage_backend_name = "influxdb"

    def __init__(
        self,
        *,
        url: str,
        bucket: str,
        token: str,
        org: str | None = None,
        precision: str = "ns",
        measure_prefix: str = "",
        write_batch: int = 5000,
        max_retries: int = 3,
        retry_base_delay: float = 5.0,
        root_dir: str | Path = ".mmdpo_cache",
        provider_hint: str = "thetadata",
    ) -> None:
        """Initialize backend using legacy manager configuration.

        Parameters
        ----------
        url : str
            InfluxDB host URL.
        bucket : str
            InfluxDB database/bucket.
        token : str
            InfluxDB token.
        org : str | None, optional
            Optional org value kept for compatibility.
        precision : str, optional
            Timestamp precision hint.
        measure_prefix : str, optional
            Measurement prefix used by legacy writer.
        write_batch : int, optional
            Batch size for writes.
        max_retries : int, optional
            Max retries for failed batches.
        retry_base_delay : float, optional
            Base delay for retry backoff.
        root_dir : str | Path, optional
            Runtime root directory used by legacy cache/log paths.
        provider_hint : str, optional
            Provider value used when rebuilding dataset keys from inventory.
        """
        self.org = org
        self.precision = precision
        self.provider_hint = provider_hint

        cfg = ManagerConfig(
            root_dir=str(root_dir),
            max_concurrency=1,
            influx_url=url,
            influx_bucket=bucket,
            influx_token=token,
            influx_precision=precision,
            influx_measure_prefix=measure_prefix,
            influx_write_batch=write_batch,
        )
        setattr(cfg, "influx_max_retries", max_retries)
        setattr(cfg, "influx_retry_delay", retry_base_delay)

        self._manager = ThetaSyncManager(cfg, _NoopThetaClient())
        self.failed_batch_dir = Path(cfg.root_dir) / cfg.cache_dir_name / "failed_influx_batches"

    @staticmethod
    def _parse_dataset_key(dataset_key: str) -> DatasetKeyParts:
        """Parse canonical dataset key.

        Parameters
        ----------
        dataset_key : str
            Canonical key:
            `{provider}:{asset}:{symbol}:{interval}:{storage_backend}`.

        Returns
        -------
        DatasetKeyParts
            Parsed key fields.
        """
        parts = str(dataset_key).split(":")
        if len(parts) != 5:
            raise ValueError(
                "Invalid dataset_key format. Expected "
                "'provider:asset:symbol:interval:storage_backend'."
            )
        provider, asset, symbol, interval, storage_backend = parts
        return DatasetKeyParts(
            provider=provider,
            asset=asset,
            symbol=symbol,
            interval=interval,
            storage_backend=storage_backend.lower(),
        )

    def _build_base_path(self, key: DatasetKeyParts) -> str:
        """Build legacy base path used to derive measurement names.

        Parameters
        ----------
        key : DatasetKeyParts
            Parsed dataset key.

        Returns
        -------
        str
            Legacy-format synthetic base path with `.influxdb` extension.
        """
        start_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        return self._manager._make_file_basepath(
            asset=key.asset,
            symbol=key.symbol,
            interval=key.interval,
            start_iso=start_iso,
            ext="influxdb",
        )

    async def write_records(
        self,
        *,
        dataset_key: str,
        records: Sequence[Mapping[str, Any]],
        mode: str = "append",
    ) -> int:
        """Persist records through legacy Influx append logic.

        Parameters
        ----------
        dataset_key : str
            Canonical dataset key.
        records : Sequence[Mapping[str, Any]]
            Records to write.
        mode : str, optional
            Supported value is `append`; `overwrite` is not implemented.

        Returns
        -------
        int
            Number of written points.
        """
        if not records:
            return 0

        key = self._parse_dataset_key(dataset_key)
        if key.storage_backend != "influxdb":
            raise NotImplementedError(
                "InfluxDBStorageBackend accepts only keys ending with ':influxdb'."
            )

        mode_lower = str(mode).strip().lower()
        if mode_lower not in {"append", "overwrite"}:
            raise ValueError("Unsupported mode. Allowed: append | overwrite")
        if mode_lower == "overwrite":
            raise NotImplementedError(
                "Overwrite mode is not implemented for InfluxDB backend. "
                "Legacy flow writes in append mode with logical dedupe."
            )

        df_new = pd.DataFrame([dict(row) for row in records]) if records else pd.DataFrame()
        if df_new.empty:
            return 0

        base_path = self._build_base_path(key)
        return await self._manager._append_influx_df(base_path, df_new)

    async def read_records(
        self,
        *,
        dataset_key: str,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read records using legacy query path for InfluxDB sink.

        Parameters
        ----------
        dataset_key : str
            Canonical dataset key.
        start_date : str | None, optional
            Inclusive lower date bound (`YYYY-MM-DD`).
        end_date : str | None, optional
            Inclusive upper date bound (`YYYY-MM-DD`).
        limit : int | None, optional
            Max rows to return.

        Returns
        -------
        list[dict[str, Any]]
            Retrieved records.
        """
        key = self._parse_dataset_key(dataset_key)
        if key.storage_backend != "influxdb":
            raise NotImplementedError(
                "InfluxDBStorageBackend accepts only keys ending with ':influxdb'."
            )

        def _read() -> list[dict[str, Any]]:
            df, _warnings = self._manager.query_local_data(
                asset=key.asset,
                symbol=key.symbol,
                interval=key.interval,
                sink="influxdb",
                start_date=start_date,
                end_date=end_date,
                max_rows=limit,
            )
            if df is None or df.empty:
                return []
            out = df.copy()
            out = out.where(pd.notna(out), None)
            return list(out.to_dict(orient="records"))

        return await asyncio.to_thread(_read)

    async def list_series(self) -> Sequence[str]:
        """List available series by reusing legacy sink inventory logic.

        Returns
        -------
        Sequence[str]
            Canonical dataset keys for InfluxDB series.
        """
        def _list() -> list[str]:
            table = self._manager.list_available_data(sink="influxdb")
            if table is None or table.empty:
                return []

            out: set[str] = set()
            needed = {"asset", "symbol", "interval"}
            if not needed.issubset(set(table.columns)):
                return []

            for row in table.itertuples(index=False):
                asset = getattr(row, "asset", None)
                symbol = getattr(row, "symbol", None)
                interval = getattr(row, "interval", None)
                if asset and symbol and interval:
                    out.add(f"{self.provider_hint}:{asset}:{symbol}:{interval}:influxdb")
            return sorted(out)

        return await asyncio.to_thread(_list)

    def _ensure_influx_client(self):
        """Proxy legacy Influx client initialization.

        Returns
        -------
        Any
            InfluxDB client object from legacy manager.
        """
        return self._manager._ensure_influx_client()

    async def _append_influx_df(self, base_path: str, df_new) -> int:
        """Proxy legacy append writer (same signature as legacy manager).

        Parameters
        ----------
        base_path : str
            Legacy base path used to derive measurement name.
        df_new : Any
            DataFrame-like object to write.

        Returns
        -------
        int
            Number of points written.
        """
        return await self._manager._append_influx_df(base_path, df_new)

    def _filter_df_against_influx_existing(
        self,
        df: pd.DataFrame,
        measurement: str,
        interval: str,
    ) -> pd.DataFrame:
        """Proxy legacy DB-level dedupe by logical keys."""
        return self._manager._filter_df_against_influx_existing(df, measurement, interval)

    def _influx_existing_keys_between(
        self,
        measurement: str,
        interval: str,
        start_utc: pd.Timestamp,
        end_utc: pd.Timestamp,
        key_cols: list[str],
    ) -> set[tuple]:
        """Proxy legacy key lookup on Influx for recovery/dedupe workflows."""
        return self._manager._influx_existing_keys_between(
            measurement,
            interval,
            start_utc,
            end_utc,
            key_cols,
        )

    def _influx_day_has_any(self, measurement: str, day_iso: str) -> bool:
        """Proxy legacy fast day-presence check."""
        return self._manager._influx_day_has_any(measurement, day_iso)

    def _influx_first_ts_for_et_day(self, measurement: str, day_iso: str):
        """Proxy legacy first-timestamp lookup for ET day."""
        return self._manager._influx_first_ts_for_et_day(measurement, day_iso)

    def _influx_last_ts_between(
        self,
        measurement: str,
        start_utc: pd.Timestamp,
        end_utc: pd.Timestamp,
    ):
        """Proxy legacy last-timestamp lookup in UTC range."""
        return self._manager._influx_last_ts_between(measurement, start_utc, end_utc)

    def _influx_available_dates_get_all(self, measurement: str) -> set[str]:
        """Proxy legacy available-day inventory lookup."""
        return self._manager._influx_available_dates_get_all(measurement)

    def _influx_available_dates_note_days(self, measurement: str, day_isos: set[str]) -> None:
        """Proxy legacy available-day index update."""
        self._manager._influx_available_dates_note_days(measurement, day_isos)

    def recover_failed_batches(
        self,
        *,
        dry_run: bool = False,
        delete_recovered: bool = True,
    ) -> None:
        """Recover failed Influx write batches from temporary storage.

        Parameters
        ----------
        dry_run : bool, optional
            If True, only reports recovery actions.
        delete_recovered : bool, optional
            If True, removes recovered JSON files.
        """
        recover_failed_batches(
            failed_batch_dir=str(self.failed_batch_dir),
            client=self._manager._ensure_influx_client(),
            dry_run=dry_run,
            delete_recovered=delete_recovered,
        )

    def list_failed_batches(self) -> tuple[list[str], list[str]]:
        """Return pending and recovered failed-batch files.

        Returns
        -------
        tuple[list[str], list[str]]
            `(pending_json_files, recovered_files)`.
        """
        return list_failed_batches(str(self.failed_batch_dir))

    def delete_failed_batch(
        self,
        *,
        measurement: str | None = None,
        batch_idx: int | None = None,
        timestamp: str | None = None,
        filename: str | None = None,
        only_recovered: bool = False,
    ) -> bool:
        """Delete one failed-batch artifact.

        Parameters
        ----------
        measurement : str | None, optional
            Measurement token used by filename matcher.
        batch_idx : int | None, optional
            Batch index token used by filename matcher.
        timestamp : str | None, optional
            Optional exact timestamp token (`YYYYMMDD_HHMMSS`).
        filename : str | None, optional
            Explicit filename/path to delete.
        only_recovered : bool, optional
            If True, restrict deletion to `.recovered` artifacts.

        Returns
        -------
        bool
            True if a file was deleted.
        """
        return delete_failed_batch(
            failed_batch_dir=str(self.failed_batch_dir),
            measurement=measurement,
            batch_idx=batch_idx,
            timestamp=timestamp,
            filename=filename,
            only_recovered=only_recovered,
        )
