# src/etl/modules/dask_runtime.py
import os
import logging
import tempfile
from pathlib import Path
from contextlib import contextmanager
from dask.distributed import Client
from distributed import LocalCluster

# -------------------------------------------------------------------
# 1) Dossier temporaire fiable (écriture ok) pour spill et config
# -------------------------------------------------------------------
def _resolve_local_dir() -> str:
    for p in (
        os.environ.get("DASK_TEMPORARY_DIRECTORY"),
        os.path.abspath(os.path.join(os.getcwd(), "dask_tmp")),
        os.path.join(tempfile.gettempdir(), "dask_tmp"),
    ):
        if not p:
            continue
        try:
            path = Path(p)
            path.mkdir(parents=True, exist_ok=True)
            t = path / ".write_test"
            t.write_text("ok", encoding="utf-8")
            t.unlink(missing_ok=True)
            return str(path)
        except Exception:
            continue
    # dernier recours : %TEMP%
    fallback = os.path.join(tempfile.gettempdir(), "dask_tmp")
    Path(fallback).mkdir(parents=True, exist_ok=True)
    return fallback

# -------------------------------------------------------------------
# 2) Crée un dask.yaml *silencieux* et l'impose aux sous-processus
#    (les workers lisent DASK_CONFIG au démarrage)
# -------------------------------------------------------------------
def _ensure_quiet_dask_yaml(config_dir: str) -> None:
    cfg_dir = Path(config_dir)
    cfg_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = cfg_dir / "dask.yaml"
    # Niveau de logs minimal partout, coupe dashboard/tornado/bokeh
    yaml_content = """\
logging:
  distributed: error
  distributed.worker: error
  distributed.scheduler: error
  distributed.nanny: error
  distributed.comm: error
  tornado: critical
  bokeh: critical
"""
    # Écrire seulement si absent ou différent
    try:
        if (not yaml_path.exists()) or (yaml_path.read_text(encoding="utf-8") != yaml_content):
            yaml_path.write_text(yaml_content, encoding="utf-8")
        # Forcer Dask à utiliser CE dossier de config
        os.environ["DASK_CONFIG"] = str(cfg_dir)
    except Exception:
        # Si la config échoue, on continue (on a d'autres garde-fous)
        pass

# -------------------------------------------------------------------
# 3) Filtre de sécurité côté parent (au cas où)
# -------------------------------------------------------------------
class _DropNoisyRecords(logging.Filter):
    TOKENS = (
        "tcp://", "Start Nanny", "Start worker", "Listening to:",
        "dashboard at", "Registered to:", "Worker plugin shuffle",
        "Waiting to connect to:", "Stopping worker", "Retire worker",
        "Remove worker",
    )
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        return not any(tok in msg for tok in self.TOKENS)

def _silence_parent_loggers(level=logging.ERROR):
    noisy = [
        "distributed", "distributed.client", "distributed.scheduler",
        "distributed.worker", "distributed.nanny", "distributed.comm",
        "distributed.http", "bokeh", "tornado",
    ]
    for name in noisy:
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.propagate = False
        if not lg.handlers:
            lg.addHandler(logging.NullHandler())
    logging.getLogger().addFilter(_DropNoisyRecords())

@contextmanager
def _suppress_all_logging():
    prev = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(prev)

# -------------------------------------------------------------------
# 4) Point d'entrée: Client silencieux
# -------------------------------------------------------------------
def _start_client():
    # Par défaut sobres pour RAM faible
    n_workers = int(os.environ.get("DASK_WORKERS", str(min(2, (os.cpu_count() or 2)))))
    threads_per_worker = int(os.environ.get("DASK_THREADS_PER_WORKER", "1"))
    memory_limit = os.environ.get("DASK_MEMORY_LIMIT", "900MB")

    local_dir = _resolve_local_dir()

    # Préparer et activer une config Dask *silencieuse* pour les sous-processus
    cfg_dir = os.path.join(local_dir, "dask_config")
    _ensure_quiet_dask_yaml(cfg_dir)

    # Réduire aussi les logs dans le process parent
    _silence_parent_loggers(logging.ERROR)

    # Créer le cluster en "mode muet" pour éviter le flot au bootstrap
    with _suppress_all_logging():
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            processes=True,                 # garde le parallélisme multi-processus
            memory_limit=memory_limit,
            local_directory=local_dir,
            dashboard_address=None,         # pas de dashboard
            silence_logs=logging.ERROR,     # workers/scheduler en ERROR
        )
    return Client(cluster)

def quiet_close(client: Client | None):
    if client is None:
        return
    with _suppress_all_logging():
        try:
            if getattr(client, "cluster", None):
                try:
                    client.cluster.close(timeout=2)
                except Exception:
                    pass
            client.close(timeout=2)
        except Exception:
            pass