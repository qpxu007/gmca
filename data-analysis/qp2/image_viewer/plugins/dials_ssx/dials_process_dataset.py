# qp2/image_viewer/plugins/dials/dials_process_dataset.py
import argparse
import json
import logging
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path


# Add qp2 to path to import logging config
def find_qp2_parent(file_path):
    path = os.path.abspath(file_path)
    while path != os.path.dirname(path):
        if os.path.basename(path) == "qp2":
            return os.path.dirname(path)
        path = os.path.dirname(path)
    return None


project_root = find_qp2_parent(__file__)
if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

from qp2.log.logging_config import setup_logging, get_logger
from qp2.config.programs import ProgramConfig

logger = get_logger(__name__)

# This script template is based on your provided example
dials_ssx_commands_template = """#!/bin/bash
echo "Starting DIALS SSX processing on $(date) at $(hostname)"

set -e
{ProgramConfig.get_setup_command('dials')}

cd {wdir}

echo "--- Running dials.import ---"
dials.import template={master_file} output.experiments=imported.expt

echo "--- Running dials.find_spots ---"
dials.find_spots imported.expt nproc={nproc} {d_min_param}

echo "--- Running dials.ssx_index ---"
dials.ssx_index imported.expt strong.refl max_lattices=2 nproc={nproc} {unit_cell_param} {space_group_param} {extra_options}

echo "--- Running dials.ssx_integrate ---"
dials.ssx_integrate indexed.expt indexed.refl nproc={nproc}

echo "DIALS command script finished successfully."
"""


def run_bash_script(script_path, cwd):
    logger.info(f"Executing bash script in {cwd}: {script_path}")
    # Use /bin/bash to explicitly run the script, preventing Exec format errors
    cmd = ["/bin/bash", str(script_path)]
    with open(Path(cwd) / "dials_job.out", "w") as out_f, open(
            Path(cwd) / "dials_job.err", "w"
    ) as err_f:
        result = subprocess.run(cmd, cwd=cwd, stdout=out_f, stderr=err_f, text=True)

    if result.returncode != 0:
        with open(Path(cwd) / "dials_job.err", "r") as err_f:
            stderr_content = err_f.read()
        raise RuntimeError(
            f"DIALS script failed: {script_path}\n\nSTDERR:\n{stderr_content}"
        )
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run DIALS SSX processing on a full dataset."
    )
    parser.add_argument("--master_file", required=True)
    parser.add_argument("--proc_dir", required=True)
    parser.add_argument("--redis_key", required=True)
    parser.add_argument("--status_key", required=True)
    parser.add_argument("--nproc", type=int, default=8)
    parser.add_argument("--d_min", type=float, default=None)
    parser.add_argument("--space_group", type=str, default="")
    parser.add_argument("--unit_cell", type=str, default="")
    parser.add_argument("--extra_options", type=str, default="")
    parser.add_argument("--redis_host", type=str, required=True)
    parser.add_argument("--redis_port", type=int, default=6379)
    args = parser.parse_args()

    setup_logging(root_name="qp2", log_level=logging.INFO)
    redis_conn = redis.Redis(
        host=args.redis_host, port=args.redis_port, decode_responses=True
    )

    try:
        wdir = Path(args.proc_dir)
        wdir.mkdir(parents=True, exist_ok=True)

        # Prepare the bash script from the template
        cmd_script = dials_ssx_commands_template.format(
            wdir=wdir,
            master_file=args.master_file,
            nproc=args.nproc,
            d_min_param=f"d_min={args.d_min}" if args.d_min else "",
            space_group_param=(
                f"space_group={args.space_group}" if args.space_group else ""
            ),
            unit_cell_param=f"unit_cell='{args.unit_cell}'" if args.unit_cell else "",
            extra_options=" ".join(
                shlex.split(args.extra_options)
            ),  # Pass extra options
        )
        script_path = wdir / "run_dials_ssx.sh"
        with open(script_path, "w") as f:
            f.write(cmd_script)
        os.chmod(script_path, 0o755)

        # Execute the script
        run_bash_script(script_path, wdir)

        # --- Parse the results as per your workflow ---
        # DIALS imports must be available in this Python environment
        from dials.array_family import flex
        from dxtbx.model.experiment_list import ExperimentListFactory

        # 1. Parse strong.refl for spot counts
        strong_refl_path = wdir / "strong.refl"
        if not strong_refl_path.exists():
            raise FileNotFoundError(
                "dials.find_spots did not produce strong.refl. No spots found."
            )

        strong_rt = flex.reflection_table.from_file(str(strong_refl_path))
        logger.info(f"DIALS found {len(strong_rt)} total spots.")

        # 2. Parse indexed.expt and indexed.refl for indexing results
        indexed_expt_path = wdir / "indexed.expt"
        indexed_refl_path = wdir / "indexed.refl"
        indexed_crystals = {}
        if indexed_expt_path.exists():
            indexed_expts = ExperimentListFactory.from_json_file(
                str(indexed_expt_path), check_format=False
            )
            for i, crystal in enumerate(indexed_expts.crystals()):
                indexed_crystals[i] = crystal

        # 3. Aggregate data per frame and save to Redis
        results_by_frame = {}
        imageset = ExperimentListFactory.from_json_file(
            str(wdir / "imported.expt"), check_format=False
        )[0].imageset
        total_frames = len(imageset)

        for i in range(total_frames):
            frame_num_1based = i + 1
            frame_spots = strong_rt.select(
                strong_rt["xyzobs.px.value"].parts()[2] == float(i)
            )

            result_dict = {
                "img_num": frame_num_1based,
                "num_spots_dials": len(frame_spots),
                "dials_indexed": False,
                "timestamp": time.time(),
            }

            if i in indexed_crystals:
                crystal = indexed_crystals[i]
                result_dict["dials_indexed"] = True
                result_dict["unit_cell_dials"] = list(
                    crystal.get_unit_cell().parameters()
                )
                result_dict["space_group_dials"] = (
                    crystal.get_space_group().info().symbol_and_number()
                )

            results_by_frame[frame_num_1based] = result_dict

        if redis_conn:
            # Clear old results first, then write new ones
            redis_conn.delete(args.redis_key)
            with redis_conn.pipeline() as pipe:
                for frame_num, data in results_by_frame.items():
                    pipe.hset(args.redis_key, frame_num, json.dumps(data))
                pipe.execute()
            logger.info(
                f"Successfully saved results for {len(results_by_frame)} frames to Redis."
            )

        final_status = {"status": "COMPLETED", "timestamp": time.time()}
        redis_conn.set(args.status_key, json.dumps(final_status), ex=24 * 3600)

    except Exception as e:
        logger.error(f"DIALS dataset process failed: {e}", exc_info=True)
        if redis_conn:
            failed_status = {
                "status": "FAILED",
                "timestamp": time.time(),
                "error": str(e),
            }
            redis_conn.set(args.status_key, json.dumps(failed_status), ex=24 * 3600)
        sys.exit(1)


if __name__ == "__main__":
    main()
