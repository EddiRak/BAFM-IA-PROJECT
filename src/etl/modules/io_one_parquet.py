# src/etl/modules/io_one_parquet.py
import os, glob, shutil
import dask.dataframe as dd

def write_one_parquet(ddf: dd.DataFrame, out_file: str, compression="snappy"):
    """
    Écrit un DataFrame Dask en parquet.
    - Par défaut: mono-fichier (comportement historique).
    - Si RAW_SINGLE_FILE=0 : dossier parquet (multi-fichiers), plus rapide et scalable.
    """
    single_file = os.environ.get("RAW_SINGLE_FILE", "1") != "0"

    if not single_file:
        out_dir = out_file if out_file.lower().endswith(".parquet") else out_file + ".parquet"
        ddf.to_parquet(out_dir, write_index=False, compression=compression, engine="pyarrow")
        return

    tmpdir = out_file + ".tmpdir"
    ddf = ddf.repartition(npartitions=1)
    ddf.to_parquet(tmpdir, write_index=False, compression=compression, engine="pyarrow")
    part_files = glob.glob(os.path.join(tmpdir, "*.parquet"))
    if not part_files:
        raise RuntimeError(f"Aucun part parquet dans {tmpdir}")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    shutil.move(part_files[0], out_file)
    shutil.rmtree(tmpdir, ignore_errors=True)
