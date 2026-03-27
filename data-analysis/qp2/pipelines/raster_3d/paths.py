from pathlib import Path


def get_raster_3d_pair_name(run1_prefix: str, run2_prefix: str) -> str:
    return f"{run1_prefix}__{run2_prefix}"


def get_raster_3d_proc_dir(proc_root_dir: str, run1_prefix: str, run2_prefix: str) -> Path:
    return Path(proc_root_dir).expanduser().resolve() / "raster_3d" / get_raster_3d_pair_name(
        run1_prefix,
        run2_prefix,
    )
