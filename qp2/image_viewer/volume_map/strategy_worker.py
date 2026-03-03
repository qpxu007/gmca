# qp2/image_viewer/volume_map/strategy_worker.py
import json
import os
import sys
from pathlib import Path

from PyQt5.QtCore import QRunnable, QObject, pyqtSignal

from qp2.image_viewer.utils.run_job import run_command
from qp2.log.logging_config import get_logger
from qp2.xio.hdf5_manager import HDF5Reader

logger = get_logger(__name__)


class StrategySignals(QObject):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(str)


class StrategyWorker(QRunnable):
    """Runs the unified strategy script for a pair of frames."""

    def __init__(
        self,
        reader_xy: HDF5Reader,
        frame_xy: int,
        reader_xz: HDF5Reader,
        frame_xz: int,
        program: str = "xds",
    ):
        super().__init__()
        self.signals = StrategySignals()
        self.reader_xy = reader_xy
        self.frame_xy = frame_xy  # 0-based
        self.reader_xz = reader_xz
        self.frame_xz = frame_xz  # 0-based
        self.program = program

    def _find_strategy_script(self) -> str:
        """Finds the path to the main.py strategy script."""
        # This assumes a standard project structure. Adjust if necessary.
        current_dir = Path(__file__).parent
        # Navigate up from qp2/image_viewer/volume_map to the project root where qp2 is
        project_root = current_dir.parent.parent.parent
        script_path = project_root / "qp2" / "strategy" / "main.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Strategy script not found at {script_path}")
        return str(script_path)

    def run(self):
        try:
            self.signals.progress.emit(
                f"Starting strategy calculation with {self.program}..."
            )

            # 1. Define a unique working directory
            base_dir = Path(self.reader_xy.master_file_path).parent
            workdir = base_dir / f"strategy_{self.frame_xy + 1}_{self.frame_xz + 1}"
            workdir.mkdir(exist_ok=True)

            strategy_script_path = self._find_strategy_script()

            # 2. Construct the command list for main.py
            cmd_list = [
                sys.executable,
                strategy_script_path,
                "--program",
                self.program,
                "--workdir",
                str(workdir),
                "--frames",
                f"{self.reader_xy.master_file_path}:{self.frame_xy + 1}",
                "--frames",
                f"{self.reader_xz.master_file_path}:{self.frame_xz + 1}",
            ]

            # 3. Execute the command using the existing utility
            # This is a blocking call within the worker thread, which is what we want.
            run_command(
                cmd=cmd_list,
                cwd=str(workdir),
                job_name="strategy_run",
                method="shell",
                background=False,
            )

            # 4. Read the JSON output from the output file
            output_file = workdir / "strategy_run.out"
            if not output_file.exists():
                raise FileNotFoundError(
                    "Strategy script did not produce an output file."
                )

            with open(output_file, "r") as f:
                output_content = f.read()

            # The script prints JSON, so we find and parse it.
            # This handles potential warnings or other text printed before the JSON.
            json_start = output_content.find("{")
            json_end = output_content.rfind("}") + 1
            if json_start == -1 or json_end == 0:
                raise ValueError(
                    f"Could not find JSON in strategy output:\n{output_content}"
                )

            result_data = json.loads(output_content[json_start:json_end])
            self.signals.finished.emit(result_data)

        except Exception as e:
            logger.error(f"Strategy worker failed: {e}", exc_info=True)
            self.signals.error.emit(str(e))
