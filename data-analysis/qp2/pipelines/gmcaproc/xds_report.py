#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A module for parsing XDS output files, running related statistics programs,
and generating a self-contained, interactive HTML report with Plotly.js.

This version includes enhanced plotting aesthetics for a cleaner, more
professional presentation.

NB: Some parser functions are derived from xdsapp 2.0 (xdsapp@helmholtz-berlin.de).
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from math import cos, radians, sqrt
from typing import Any, Dict, List, Optional, Union

try:
    from qp2.image_viewer.utils.run_job import run_command
except ImportError:
    print(
        "Error: 'run_job.py' not found. Please ensure it is in the same directory or PYTHONPATH."
    )
    sys.exit(1)

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class XDSReportGenerator:
    """
    Parses XDS output, runs statistics jobs, and creates a graphical HTML report.
    """

    # Define constants for file names
    INTEGRATE_LP = "INTEGRATE.LP"
    CORRECT_LP = "CORRECT.LP"
    XDSSTAT_LP = "XDSSTAT.LP"
    MAXCC12_LP = "MAXCC12.LP"
    XDSCC12_LP = "XDSCC12.LP"
    XDS_ASCII_HKL = "XDS_ASCII.HKL"

    # Define a professional color palette for plots
    PLOTLY_COLORS = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]

    def __init__(
            self,
            work_dir: str,
            high_res: Optional[float] = None,
            low_res: Optional[float] = None,
            pipelinestatus_id: Optional[int] = None,
            tag_name: str = "XDS Analysis Report",
    ):
        self.work_dir = os.path.abspath(work_dir)
        self.high_res = high_res
        self.low_res = low_res
        self.pipelinestatus_id = pipelinestatus_id
        self.tag_name = tag_name
        self.stats: Dict[str, Any] = {}

    def run_statistics_jobs(
            self, xdsstat_bin="xdsstat", xdscc12_bin="xdscc12", maxcc12_bin="xscale_maxcc12"
    ) -> bool:
        """Runs XDS statistics programs if XDS_ASCII.HKL exists."""
        refl_file = os.path.join(self.work_dir, self.XDS_ASCII_HKL)
        if not os.path.exists(refl_file):
            logger.warning(
                f"{refl_file} does not exist. Skipping statistics job execution."
            )
            return False

        logger.info("Running XDS statistics programs...")
        res_range = (
            f"{self.low_res} {self.high_res}" if self.high_res and self.low_res else ""
        )
        dmax = f"-dmax {self.high_res}" if self.high_res else ""

        commands = [
            f"echo {self.XDS_ASCII_HKL} | {xdsstat_bin} {res_range}",
            f"{xdscc12_bin} {res_range} -nbin 5 -t 5 {self.XDS_ASCII_HKL}",
            f"{maxcc12_bin} {dmax} -nbin 5 {self.XDS_ASCII_HKL}",
        ]
        output_files = [self.XDSSTAT_LP, self.XDSCC12_LP, self.MAXCC12_LP]
        job_names = ["xdsstat_job", "xdscc12_job", "maxcc12_job"]

        for i, cmd in enumerate(commands):
            full_cmd = f'/bin/bash -c "{cmd} > {output_files[i]}"'
            logger.info(f"Executing: {full_cmd}")
            try:
                run_command(
                    full_cmd,
                    cwd=self.work_dir,
                    job_name=job_names[i],
                    method="shell",
                    background=False,
                )
            except (FileNotFoundError, Exception) as e:
                logger.error(
                    f"Could not run '{cmd}'. Is the program in your PATH? Error: {e}"
                )
                return False
        return True

    def _load_all_stats(self):
        """Loads and parses all available XDS log files."""
        parsers = {
            self.INTEGRATE_LP: self._parse_integrate_lp,
            self.CORRECT_LP: self._parse_correct_lp,
            self.XDSSTAT_LP: self._parse_xdsstat_lp,
            self.MAXCC12_LP: self._parse_maxcc12_lp,
            self.XDSCC12_LP: self._parse_xdscc12_lp,
        }
        for filename, parser_func in parsers.items():
            file_path = os.path.join(self.work_dir, filename)
            if os.path.exists(file_path):
                try:
                    parsed_data = parser_func(file_path)
                    if parsed_data:
                        self.stats.update(parsed_data)
                        logger.info(f"Successfully parsed {filename}")
                    else:
                        logger.warning(f"Parsing {filename} yielded no data.")
                except Exception as e:
                    logger.error(f"Failed to parse {filename}: {e}", exc_info=True)
            else:
                logger.warning(f"File not found, skipping parsing: {filename}")

    # =========================================================================
    # PARSING METHODS
    # =========================================================================
    def _split_line(self, line, sep):
        """Splits a line by a separator and removes empty, stripped columns."""
        return [col.strip() for col in line.split(sep) if col.strip()]

    def _safe_float(self, value, default=0.0):
        """Safely convert a value to float, returning a default on failure."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value, default=0):
        """Safely convert a value to int, returning a default on failure."""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _parse_integrate_lp(self, file_path: str) -> Dict[str, Any]:
        """Parses an INTEGRATE.LP file."""
        params = {}
        batch = 0
        with open(file_path, "r") as file_open:
            for line in file_open:
                cols = self._split_line(line, " ")
                if "PROCESSING OF IMAGES" in line:
                    cols_line = line.split()
                    params.setdefault("integrate_frame_range", []).append(
                        f"{cols_line[-3]}-{cols_line[-1]}"
                    )
                elif "DEVIATION OF SPOT" in line:
                    batch += 1
                    params.setdefault("integrate_std_spotpos", []).append(
                        self._safe_float(cols[6])
                    )
                    params.setdefault("integrate_batch", []).append(str(batch))
                elif "DEVIATION OF SPINDLE" in line:
                    params.setdefault("integrate_std_spindle", []).append(
                        self._safe_float(cols[6])
                    )
                elif "UNIT CELL PARAMETERS" in line and len(cols) > 8:
                    params.setdefault("integrate_uca", []).append(
                        self._safe_float(cols[3])
                    )
                    params.setdefault("integrate_ucb", []).append(
                        self._safe_float(cols[4])
                    )
                    params.setdefault("integrate_ucc", []).append(
                        self._safe_float(cols[5])
                    )
                    params.setdefault("integrate_ucal", []).append(
                        self._safe_float(cols[6])
                    )
                    params.setdefault("integrate_ucbe", []).append(
                        self._safe_float(cols[7])
                    )
                    params.setdefault("integrate_ucga", []).append(
                        self._safe_float(cols[8])
                    )
                elif "CRYSTAL TO DETECTOR DISTANCE (mm)" in line:
                    params.setdefault("distance", []).append(self._safe_float(cols[-1]))
                elif "REFLECTIONS ACCEPTED FOR REFINEMENT" in line and len(cols) > 3:
                    try:
                        params.setdefault("integrate_accepted", []).append(
                            self._safe_float(cols[0])
                            / self._safe_float(cols[3], 1.0)
                            * 100
                        )
                    except ZeroDivisionError:
                        pass
                elif len(cols) == 10 and cols[1] == "0" and "-" not in line:
                    params.setdefault("integrate_frame_no", []).append(cols[0])
                    params.setdefault("integrate_scale", []).append(
                        self._safe_float(cols[2])
                    )
                    params.setdefault("integrate_nstrong", []).append(
                        self._safe_int(cols[6])
                    )
                    params.setdefault("integrate_nrej", []).append(
                        self._safe_int(cols[7])
                    )
                    params.setdefault("integrate_sigmab", []).append(
                        self._safe_float(cols[8])
                    )
                    params.setdefault("integrate_sigmar", []).append(
                        self._safe_float(cols[9])
                    )
        return params

    def _parse_correct_lp(self, file_path: str) -> Dict[str, Any]:
        """
        Parses CORRECT.LP using the original two-pass logic.
        This is intentionally preserved as it is proven to be robust against
        formatting variations in different XDS versions.
        """
        params = {}
        # First pass to get sg_no, which is needed for a later parsing step.
        with open(file_path, "r") as file_open:
            for line in file_open:
                cols = self._split_line(line, " ")
                if "SPACE_GROUP_NUMBER=" in line:
                    params["sg_no"] = cols[1]

        # Second, main parsing pass
        with open(file_path, "r") as file_open:
            for line in file_open:
                cols = self._split_line(line, " ")
                if len(cols) >= 14 and cols[0] != "RESOLUTION" and cols[0] == "total":
                    params.setdefault("r_meas_totals", []).append(
                        cols[9].replace("%", "")
                    )
                    params.setdefault("r_factor_totals", []).append(
                        cols[5].replace("%", "")
                    )
                    params.setdefault("i_sig_totals", []).append(cols[8])
                    params.setdefault("comp_totals", []).append(
                        cols[4].replace("%", "")
                    )
                    params.setdefault("uniques_totals", []).append(cols[2])
                    params.setdefault("observed_totals", []).append(cols[1])
                    params.setdefault("sig_ano_totals", []).append(cols[12])
                    params.setdefault("anom_corr_totals", []).append(cols[11])
                elif len(cols) >= 14 and cols[0] != "RESOLUTION" and cols[0] != "total":
                    params.setdefault("i_sig", []).append(cols[8])
                    params.setdefault("reso_shell", []).append(cols[0])
                    params.setdefault("sig_ano", []).append(cols[12])
                    params.setdefault("cc_half", []).append(cols[10])
                    params.setdefault("r_factor", []).append(cols[5].replace("%", ""))
                    params.setdefault("r_meas", []).append(cols[9].replace("%", ""))
                    params.setdefault("anom_corr", []).append(cols[11].replace("%", ""))
                    params.setdefault("comp", []).append(cols[4].replace("%", ""))
                elif "UNIT_CELL_CONSTANTS=" in line:
                    params["uca"], params["ucb"], params["ucc"] = (
                        cols[1],
                        cols[2],
                        cols[3],
                    )
                    params["alpha"] = self._safe_float(cols[4])
                    params["beta"] = self._safe_float(cols[5])
                    params["gamma"] = self._safe_float(cols[6])
                elif ("E+" in line or "E-" in line) and len(cols) == 3:
                    params["avalue"], params["bvalue"], params["isa"] = (
                        cols[0],
                        cols[1],
                        cols[2],
                    )

        # Post-processing to extract summary values and table slices
        try:
            params["r_meas_total"] = params["r_meas_totals"][-1]
            params["r_factor_total"] = params["r_factor_totals"][-1]
            params["i_sig_total"] = params["i_sig_totals"][-1]
            params["comp_total"] = params["comp_totals"][-1]
            params["uniques_total"] = params["uniques_totals"][-1]
            params["observed_total"] = params["observed_totals"][-1]
            if params.get("reso_shell"):
                params["res_table"] = params["reso_shell"][-9:]
            if params.get("r_meas"):
                params["r_meas_table"] = params["r_meas"][-9:]
            if params.get("r_factor"):
                params["r_factor_table"] = params["r_factor"][-9:]
            if params.get("i_sig"):
                params["i_sig_table"] = params["i_sig"][-9:]
            if params.get("comp"):
                params["comp_table"] = params["comp"][-9:]
            if params.get("sig_ano"):
                params["sig_ano_table"] = params["sig_ano"][-9:]
            if params.get("anom_corr"):
                params["anom_corr_table"] = params["anom_corr"][-9:]
            if params.get("cc_half"):
                params["cc_half_table"] = params["cc_half"][-9:]
        except (KeyError, IndexError) as e:
            logger.warning(f"Could not parse all summary stats from CORRECT.LP: {e}")

        return params

    def _parse_xdsstat_lp(self, file_path: str) -> Dict[str, Any]:
        params = defaultdict(list)
        with open(file_path, "r") as file_open:
            for line in file_open:
                cols = self._split_line(line, " ")
                if len(cols) == 12 and " L" in line:
                    params["i_over_sigma_per_frame"].append(self._safe_float(cols[5]))
                    params["cc_per_frame"].append(self._safe_float(cols[7]))
                    params["r_meas_per_frame"].append(self._safe_float(cols[8]) * 100)
                if "DIFFERENCE" in line:
                    params["xdsstat_frame_diff"].append(self._safe_int(cols[0]))
                    params["r_d"].append(self._safe_float(cols[2]) * 100)
        return dict(params)

    def _parse_maxcc12_lp(self, file_path: str) -> Dict[str, Any]:
        params = defaultdict(list)
        with open(file_path, "r") as f:
            lines = f.read().splitlines()
        for i, line in enumerate(lines):
            if line.startswith(" resolution shells:"):
                cols = lines[i + 1].split()
                lower = ["inf"] + cols[:-1]
                params["reso_shells"] = [f"{l}-{u}A" for l, u in zip(lower, cols)]
            if line.endswith("a"):
                cols = line.replace("-100.0", " -99.9").split()
                params["maxcc_frame_no"].append(cols[0])
                params["cc_half_iso"].append(cols[1:-1])
            elif line.endswith("c"):
                params["completeness"].append(line.split()[:-1])
            elif line.endswith("d"):
                cols = line.replace("-100.0", " -99.9").split()
                params["cc_half_ano"].append(cols[:-1])
        return dict(params)

    def _parse_xdscc12_lp(self, file_path: str) -> Dict[str, Any]:
        result = defaultdict(list)
        with open(file_path) as fh:
            lines = fh.read().splitlines()
        ibatch, nframes = 0, 1
        for i, line in enumerate(lines):
            if "resolution shells (for lines starting with" in line:
                cols = lines[i + 1].split()
                lower = ["inf"] + cols[:-1]
                result["xdscc12_reso_shells"] = [
                    f"{l}-{u}A" for l, u in zip(lower, cols)
                ]
            if "!OSCILLATION_RANGE=" in line:
                try:
                    nframes = int(lines[i + 1].split()[0])
                except (IndexError, ValueError):
                    nframes = 1
            cols = line.split()
            if not cols:
                continue
            if cols[0] == "a":
                result["xdscc12_deltaCC_iso"].append(self._safe_float(cols[5]))
                ibatch += 1
                result["xdscc12_frame_range"].append(
                    f"{(ibatch - 1) * nframes + 1}-{ibatch * nframes}"
                )
            elif cols[0] == "b":
                result["xdscc12_deltaCC_iso_shells"].append(cols[1:])
            elif cols[0] == "d":
                result["xdscc12_deltaCC_ano"].append(self._safe_float(cols[5]))
            elif cols[0] == "e":
                result["xdscc12_deltaCC_ano_shells"].append(cols[1:])
        return result

    # =========================================================================
    # HTML and PLOT GENERATION
    # =========================================================================
    def _get_plot_block(
            self,
            x: List,
            y: List,
            name: str,
            visible: Union[bool, str],
            color: str,
            mode: str = "lines+markers",
            hovertemplate: str = "",
    ) -> str:
        """Generates a JavaScript string for a single Plotly trace with enhanced styling."""
        return f"""{{
            x: {json.dumps(x)}, 
            y: {json.dumps(y)}, 
            mode: '{mode}',
            name: '{name}', 
            visible: {json.dumps(visible)},
            line: {{shape: 'linear', width: 2, color: '{color}'}},
            marker: {{ symbol: 'circle', size: 6, color: '{color}' }},
            hovertemplate: '{hovertemplate}'
        }}"""

    def _generate_plot_div(
            self,
            div_id: str,
            title: str,
            xlabel: str,
            ylabel: str,
            caption: str,
            data_blocks: List[str],
    ) -> str:
        """A generic helper to create a plot div with enhanced layout."""
        if not data_blocks:
            logger.warning(f"No data blocks provided for plot '{title}'. Skipping.")
            return ""

        traces = ",\n".join(data_blocks)
        return f"""
        <div class="plot-container" id="{div_id}">
            <a class="anchor" id="anchor_{div_id}"></a>
            <h2>{title}</h2>
            <div id="plot_{div_id}" class="plotly-chart"></div>
            <p class="caption">{caption}</p>
        </div>
        <script>
        (function() {{
            var data = [{traces}];
            var layout = {{
                font: {{ family: 'Arial, sans-serif' }},
                title: {{ text: '{title}', font: {{ size: 20, color: '#333' }}, x: 0.5 }},
                xaxis: {{
                    title: '{xlabel}',
                    titlefont: {{ size: 16 }}, tickfont: {{ size: 12 }},
                    gridcolor: '#e8e8e8', gridwidth: 1
                }},
                yaxis: {{
                    title: '{ylabel}',
                    titlefont: {{ size: 16 }}, tickfont: {{ size: 12 }},
                    gridcolor: '#e8e8e8', gridwidth: 1,
                    automargin: true
                }},
                legend: {{
                    orientation: 'v', bgcolor: 'rgba(255,255,255,0.7)',
                    bordercolor: '#ccc', borderwidth: 1,
                    font: {{ size: 11 }}
                }},
                hovermode: 'x unified',
                autosize: true,
                margin: {{ l: 80, r: 40, b: 80, t: 80, pad: 4 }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)'
            }};
            Plotly.newPlot('plot_{div_id}', data, layout, {{displayModeBar: true, responsive: true}});
        }})();
        </script>
        """

    def _plot_merging_stats(self) -> str:
        """Generates the 'Data Merging Statistics' plot."""
        if "res_table" not in self.stats or not self.stats["res_table"]:
            logger.warning("Key 'res_table' not found or empty. Skipping merging plot.")
            return ""

        resol = [f"{x}A" for x in self.stats["res_table"]]
        blocks = []

        trace_defs = [
            {"key": "cc_half_table", "name": "CC(1/2) (%)", "visible": True},
            {"key": "i_sig_table", "name": "I/sig(I)", "visible": True},
            {"key": "comp_table", "name": "Completeness (%)", "visible": True},
            {"key": "anom_corr_table", "name": "Anom. Corr. (%)", "visible": True},
            {"key": "r_meas_table", "name": "R-meas (%)", "visible": "legendonly"},
            {"key": "r_factor_table", "name": "R-factor (%)", "visible": "legendonly"},
            {"key": "sig_ano_table", "name": "SigAno", "visible": "legendonly"},
        ]

        for i, tdef in enumerate(trace_defs):
            if self.stats.get(tdef["key"]):
                y_data = [
                    self._safe_float(str(v).replace("*", ""))
                    for v in self.stats[tdef["key"]]
                ]
                hover = (
                        "Resolution: %{x}<br>" + f"{tdef['name']}: %{{y}}<extra></extra>"
                )
                blocks.append(
                    self._get_plot_block(
                        resol,
                        y_data,
                        tdef["name"],
                        tdef["visible"],
                        self.PLOTLY_COLORS[i % len(self.PLOTLY_COLORS)],
                        hovertemplate=hover,
                    )
                )

        try:
            summary = (
                f"Rsym={self.stats.get('r_factor_total', 'N/A')}, "
                f"Rmeas={self.stats.get('r_meas_total', 'N/A')}, "
                f"Completeness={self.stats.get('comp_total', 'N/A')}, "
                f"Redundancy={self._safe_float(self.stats['observed_total']) / self._safe_float(self.stats['uniques_total'], 1.0):.2f}, "
                f"ISa={self.stats.get('isa', 'N/A')}"
            )
        except (KeyError, ValueError, TypeError, ZeroDivisionError):
            summary = "Summary statistics could not be calculated."

        caption = (
            f"Merging statistics in resolution shells. Overall: {summary}. "
            f"<a href='{self.CORRECT_LP}' target='_blank'>View CORRECT.LP</a>"
        )
        return self._generate_plot_div(
            "data_quality",
            "Data Merging Statistics",
            "Resolution (Å)",
            "Value",
            caption,
            blocks,
        )

    def _plot_integration_per_frame(self) -> str:
        if "integrate_frame_no" not in self.stats:
            logger.warning(
                "Key 'integrate_frame_no' not found. Skipping per-frame plot."
            )
            return ""
        fnos = self.stats["integrate_frame_no"]
        blocks = []
        trace_defs = [
            {
                "key": "integrate_sigmar",
                "name": "Mosaicity (deg)",
                "visible": True,
                "mode": "lines",
            },
            {
                "key": "integrate_scale",
                "name": "Scale Factor",
                "visible": True,
                "mode": "lines",
            },
            {
                "key": "i_over_sigma_per_frame",
                "name": "I/sig(I)",
                "visible": "legendonly",
                "mode": "lines",
            },
            {
                "key": "r_meas_per_frame",
                "name": "R-meas (%)",
                "visible": "legendonly",
                "mode": "lines",
            },
            {
                "key": "cc_per_frame",
                "name": "Correlation",
                "visible": "legendonly",
                "mode": "lines",
            },
            {
                "key": "integrate_nstrong",
                "name": "No. Strong Spots",
                "visible": "legendonly",
                "mode": "lines",
            },
        ]
        for i, tdef in enumerate(trace_defs):
            if self.stats.get(tdef["key"]):
                min_len = min(len(fnos), len(self.stats[tdef["key"]]))
                hover = "Frame: %{x}<br>" + f"{tdef['name']}: %{{y}}<extra></extra>"
                blocks.append(
                    self._get_plot_block(
                        fnos[:min_len],
                        self.stats[tdef["key"]][:min_len],
                        tdef["name"],
                        tdef["visible"],
                        self.PLOTLY_COLORS[i % len(self.PLOTLY_COLORS)],
                        mode=tdef["mode"],
                        hovertemplate=hover,
                    )
                )

        caption = (
            f"Changes in parameters between frames during integration. "
            f"<a href='{self.INTEGRATE_LP}' target='_blank'>View INTEGRATE.LP</a>"
        )
        return self._generate_plot_div(
            "frmnos",
            "Per-Frame Integration Parameters",
            "Frame Number",
            "Value",
            caption,
            blocks,
        )

    def _plot_integration_per_batch(self) -> str:
        if "integrate_frame_range" not in self.stats:
            logger.warning(
                "Key 'integrate_frame_range' not found. Skipping per-batch plot."
            )
            return ""

        bnos = self.stats["integrate_frame_range"]
        vol_change = []
        try:
            arr_a, arr_b, arr_c = (
                self.stats["integrate_uca"],
                self.stats["integrate_ucb"],
                self.stats["integrate_ucc"],
            )
            arr_al, arr_be, arr_ga = (
                self.stats["integrate_ucal"],
                self.stats["integrate_ucbe"],
                self.stats["integrate_ucga"],
            )
            vols = [
                self._cal_volume(a, b, c, al, be, ga)
                for a, b, c, al, be, ga in zip(
                    arr_a, arr_b, arr_c, arr_al, arr_be, arr_ga
                )
            ]
            if vols:
                v0 = vols[0]
                vol_change = [100 * (v - v0) / v0 for v in vols]
                self.stats["vol_change_percent"] = vol_change
        except (KeyError, IndexError, TypeError, ZeroDivisionError) as e:
            logger.warning(f"Could not calculate cell volume change: {e}")

        blocks = []
        trace_defs = [
            {
                "key": "vol_change_percent",
                "name": "Cell Volume Change (%)",
                "visible": True,
            },
            {
                "key": "integrate_std_spotpos",
                "name": "Spot Position STD (pixel)",
                "visible": "legendonly",
            },
            {"key": "integrate_uca", "name": "Cell a (Å)", "visible": "legendonly"},
            {"key": "integrate_ucb", "name": "Cell b (Å)", "visible": "legendonly"},
            {"key": "integrate_ucc", "name": "Cell c (Å)", "visible": "legendonly"},
        ]
        for i, tdef in enumerate(trace_defs):
            if self.stats.get(tdef["key"]):
                hover = "Batch: %{x}<br>" + f"{tdef['name']}: %{{y}}<extra></extra>"
                blocks.append(
                    self._get_plot_block(
                        bnos,
                        self.stats[tdef["key"]],
                        tdef["name"],
                        tdef["visible"],
                        self.PLOTLY_COLORS[i % len(self.PLOTLY_COLORS)],
                        hovertemplate=hover,
                    )
                )

        caption = (
            f"Changes in parameters between integration segments (batches). "
            f"<a href='{self.INTEGRATE_LP}' target='_blank'>View INTEGRATE.LP</a>"
        )
        return self._generate_plot_div(
            "btchnos",
            "Per-Batch Integration Parameters",
            "Integration Batch (Frame Range)",
            "Value",
            caption,
            blocks,
        )

    def _plot_r_decay(self) -> str:
        if "xdsstat_frame_diff" not in self.stats:
            logger.warning("Key 'xdsstat_frame_diff' not found. Skipping R_d plot.")
            return ""
        hover = "Frame difference: %{x}<br>R-decay: %{y:.2f}%<extra></extra>"
        blocks = [
            self._get_plot_block(
                self.stats["xdsstat_frame_diff"],
                self.stats.get("r_d", []),
                "R_decay (%)",
                True,
                self.PLOTLY_COLORS[0],
                hovertemplate=hover,
            )
        ]
        caption = (
            "R_d as a function of frame number difference. An increasing R_d may suggest radiation damage. "
            f"<a href='{self.XDSSTAT_LP}' target='_blank'>View XDSSTAT.LP</a>"
        )
        return self._generate_plot_div(
            "R_decay",
            "R_decay (Rd) Plot",
            "Difference in Frame Number",
            "R_d (%)",
            caption,
            blocks,
        )

    def _plot_xdscc12(self) -> str:
        if "xdscc12_frame_range" not in self.stats:
            logger.warning(
                "Key 'xdscc12_frame_range' not found. Skipping delta CC1/2 plots."
            )
            return ""

        fnos = self.stats["xdscc12_frame_range"]
        html, reso_shells = "", self.stats.get("xdscc12_reso_shells", [])
        caption = (
            f"Analysis of delta CC1/2. Negative values indicate potential issues. "
            f"<a href='{self.XDSCC12_LP}' target='_blank'>View XDSCC12.LP</a>"
        )

        def generate_cc12_plot(
                data_key_overall, data_key_shells, div_id, title, ano_iso
        ):
            nonlocal html
            overall_data = self.stats.get(data_key_overall)
            if not overall_data:
                return

            hover_overall = f"Frame Range: %{{x}}<br>Delta CC1/2 ({ano_iso}): %{{y:.3f}}<extra></extra>"
            blocks = [
                self._get_plot_block(
                    fnos,
                    overall_data,
                    "Overall",
                    True,
                    self.PLOTLY_COLORS[0],
                    hovertemplate=hover_overall,
                )
            ]

            shells_data = self.stats.get(data_key_shells, [])
            if shells_data and reso_shells and len(shells_data[0]) == len(reso_shells):
                for i, shell in enumerate(reso_shells):
                    y_data = [self._safe_float(x[i]) for x in shells_data]
                    hover_shell = (
                            "Frame Range: %{x}<br>" + f"{shell}: %{{y:.3f}}<extra></extra>"
                    )
                    blocks.append(
                        self._get_plot_block(
                            fnos,
                            y_data,
                            shell,
                            "legendonly",
                            self.PLOTLY_COLORS[(i + 1) % len(self.PLOTLY_COLORS)],
                            hovertemplate=hover_shell,
                        )
                    )

            html += self._generate_plot_div(
                div_id, title, "Frame Range", "Delta CC(1/2)", caption, blocks
            )

        generate_cc12_plot(
            "xdscc12_deltaCC_iso",
            "xdscc12_deltaCC_iso_shells",
            "xdscc12_iso",
            "delta CC1/2 (iso) vs Frame Range",
            "iso",
        )
        generate_cc12_plot(
            "xdscc12_deltaCC_ano",
            "xdscc12_deltaCC_ano_shells",
            "xdscc12_ano",
            "delta CC1/2 (ano) vs Frame Range",
            "ano",
        )
        return html

    def _plot_maxcc12(self) -> str:
        if "maxcc_frame_no" not in self.stats:
            logger.warning(
                "Key 'maxcc_frame_no' not found. Skipping cumulative CC1/2 plots."
            )
            return ""

        html, fnos = "", self.stats["maxcc_frame_no"]
        start_frame = fnos[0] if fnos else "1"
        frame_ranges = [f"{start_frame}-{i}" for i in fnos]
        reso_shells = self.stats.get("reso_shells", [])

        def plot_section(
                data_key: str, div_id: str, title: str, qty_name: str, y_axis_title: str
        ):
            nonlocal html
            data_list = self.stats.get(data_key, [])
            if (
                    data_list
                    and reso_shells
                    and len(data_list) > 0
                    and len(data_list[0]) == len(reso_shells)
            ):
                blocks = []
                for i, shell in enumerate(reso_shells):
                    y_data = [self._safe_float(x[i]) for x in data_list]
                    hover = "Frames: %{x}<br>" + f"{shell}: %{{y:.2f}}<extra></extra>"
                    blocks.append(
                        self._get_plot_block(
                            frame_ranges,
                            y_data,
                            shell,
                            True,
                            self.PLOTLY_COLORS[i % len(self.PLOTLY_COLORS)],
                            mode="lines",
                            hovertemplate=hover,
                        )
                    )

                caption = (
                    f"Analysis of {qty_name} vs cumulative frames. Drops can indicate radiation damage. "
                    f"<a href='{self.MAXCC12_LP}' target='_blank'>View MAXCC12.LP</a>"
                )
                html += self._generate_plot_div(
                    div_id,
                    title,
                    "Frame Range (Cumulative)",
                    y_axis_title,
                    caption,
                    blocks,
                )

        plot_section(
            "cc_half_iso",
            "maxcc12_iso",
            "CC1/2 (iso) vs Frames (Cumulative)",
            "isomorphous CC1/2",
            "CC(1/2)",
        )
        plot_section(
            "cc_half_ano",
            "maxcc12_ano",
            "CC1/2 (ano) vs Frames (Cumulative)",
            "anomalous CC1/2",
            "CC(1/2)",
        )
        plot_section(
            "completeness",
            "maxcc12_completeness",
            "Completeness vs Frames (Cumulative)",
            "completeness",
            "Completeness (%)",
        )
        return html

    @staticmethod
    def _cal_volume(
            a: float, b: float, c: float, al: float, be: float, ga: float
    ) -> float:
        """Calculates unit cell volume."""
        cosa, cosb, cosc = [cos(radians(x)) for x in [al, be, ga]]
        return (
                a * b * c * sqrt(1 - cosa ** 2 - cosb ** 2 - cosc ** 2 + 2.0 * cosa * cosb * cosc)
        )

    def _generate_html_scaffold(self, body: str) -> str:
        """Generates the final HTML document structure."""
        reprocess_tag = (
            f"<li><a href='process:{self.pipelinestatus_id}'>Reprocess this dataset</a></li>"
            if self.pipelinestatus_id
            else ""
        )
        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>XDS Graphical Report: {self.tag_name}</title>
    <script src="https://cdn.plot.ly/plotly-2.18.2.min.js"></script>
    <style>
        :root {{
            --bg-color: #f4f7f6; --text-color: #333; --primary-color: #0056b3;
            --border-color: #dee2e6; --sidebar-bg: #ffffff; --header-bg: #e9ecef;
            --caption-bg: #eef2f7;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0; display: flex; background-color: var(--bg-color); color: var(--text-color);
            font-size: 16px;
        }}
        .sidebar {{
            width: 280px; position: fixed; height: 100vh; background-color: var(--sidebar-bg);
            border-right: 1px solid var(--border-color); overflow-y: auto; padding: 20px; box-sizing: border-box;
            box-shadow: 2px 0 5px rgba(0,0,0,0.05); z-index: 10;
        }}
        .sidebar h1 {{ font-size: 1.2rem; margin-top: 0; color: var(--primary-color); border-bottom: 1px solid var(--border-color); padding-bottom: 10px; }}
        .sidebar ul {{ list-style-type: none; padding: 0; margin: 0; }}
        .sidebar li a {{
            display: block; padding: 8px 10px; text-decoration: none; color: #343a40;
            border-radius: 4px; transition: background-color 0.2s, color 0.2s, padding-left 0.2s;
            font-size: 0.95em;
        }}
        .sidebar li a:hover {{ background-color: #e9ecef; color: var(--primary-color); padding-left: 15px;}}
        .main-content {{ margin-left: 280px; padding: 30px; width: calc(100% - 280px); box-sizing: border-box;}}
        header {{
            background-color: var(--sidebar-bg); padding: 20px; border-radius: 8px; margin-bottom: 30px;
            border: 1px solid var(--border-color); box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        header h1 {{ margin: 0; font-size: 1.8rem; }}
        header p {{ margin: 5px 0 0; color: #555; }}
        .plot-container {{
            background-color: var(--sidebar-bg); border: 1px solid var(--border-color);
            border-radius: 8px; margin-bottom: 25px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            padding: 20px;
        }}
        .plot-container h2 {{ margin-top: 0; padding-bottom: 10px; border-bottom: 1px solid var(--border-color); font-size: 1.4rem; color: #333;}}
        .caption {{
            background-color: var(--caption-bg); padding: 10px 15px; border-radius: 5px; font-size: 0.9em;
            margin: 15px 5px 5px 5px; line-height: 1.5; border-left: 3px solid var(--primary-color);
        }}
        a.anchor {{ position: relative; top: -90px; display: block; visibility: hidden; }}
        a, a:visited {{ color: var(--primary-color); text-decoration: none; font-weight: 500;}}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <nav class="sidebar">
        <h1>Table of Contents</h1>
        <ul>
            <li><a href="#anchor_data_quality">Overall Data Quality</a></li>
            <li><a href="#anchor_R_decay">R_decay Plot</a></li>
            <li><a href="#anchor_frmnos">Per-Frame Integration</a></li>
            <li><a href="#anchor_btchnos">Per-Batch Integration</a></li>
            <li><a href="#anchor_xdscc12_iso">delta CC1/2 (iso)</a></li>
            <li><a href="#anchor_xdscc12_ano">delta CC1/2 (ano)</a></li>
            <li><a href="#anchor_maxcc12_iso">CC1/2 (iso) Cumulative</a></li>
            <li><a href="#anchor_maxcc12_ano">CC1/2 (ano) Cumulative</a></li>
            <li><a href="#anchor_maxcc12_completeness">Completeness Cumulative</a></li>
            <hr style="margin: 15px 0; border: 0; border-top: 1px solid var(--border-color);">
            {reprocess_tag}
            <li><a href="XDS.INP" target="_blank">View XDS.INP</a></li>
        </ul>
    </nav>
    <main class="main-content">
        <header>
            <h1>XDS Graphical Report</h1>
            <p>{self.tag_name}</p>
        </header>
        {body}
    </main>
</body>
</html>
        """

    def create_report(self, output_filename: str = "XDS_Report.html"):
        """Generates the final HTML report file."""
        self.run_statistics_jobs()
        self._load_all_stats()
        if not self.stats:
            logger.error("No statistics could be parsed. Cannot generate report.")
            return

        plot_divs = [
            self._plot_merging_stats(),
            self._plot_r_decay(),
            self._plot_integration_per_frame(),
            self._plot_integration_per_batch(),
            self._plot_xdscc12(),
            self._plot_maxcc12(),
        ]

        report_body = "\n".join(div for div in plot_divs if div)
        if not report_body.strip():
            logger.warning("No plots were generated. The HTML report will be empty.")

        final_html = self._generate_html_scaffold(report_body)
        output_path = os.path.join(self.work_dir, output_filename)
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(final_html)
            logger.info(f"Successfully created report: {output_path}")
        except IOError as e:
            logger.error(f"Failed to write report to {output_path}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a graphical HTML report from XDS output files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--work_dir",
        type=str,
        default=os.getcwd(),
        help="Path to the XDS working directory containing the .LP files.",
    )
    parser.add_argument(
        "--tag_name",
        type=str,
        default=None,
        help="A custom name/tag for the report title. Defaults to the directory name.",
    )
    parser.add_argument(
        "--output_filename",
        type=str,
        default="XDS_Report.html",
        help="Name of the output HTML report file.",
    )
    args = parser.parse_args()

    print("--- Running XDS Report Generator ---")
    print(f"Analyzing XDS results in: {args.work_dir}")

    tag_name = args.tag_name
    if not tag_name:
        try:
            tag_name = os.path.basename(os.path.normpath(args.work_dir))
        except Exception:
            tag_name = "XDS Analysis"

    report_generator = XDSReportGenerator(work_dir=args.work_dir, tag_name=tag_name)

    report_generator.create_report(output_filename=args.output_filename)

    print(f"--- Report generation finished. ---")
    print(
        f"Open the following file in a web browser: {os.path.join(args.work_dir, args.output_filename)}"
    )
