# qp2/autoproc_xia2/autoproc_xml_parser.py
import logging
import os
import xml.etree.ElementTree as ET
from typing import Dict, Any

logger = logging.getLogger(__name__)


class AutoPROCXmlParser:
    """Parses an autoPROC.xml file to extract key statistics."""

    def __init__(self, wdir: str = ".", filename: str = "autoPROC.xml"):
        self.filepath = os.path.join(os.path.abspath(wdir), filename)
        self.tree = None
        self.root = None
        try:
            self.tree = ET.parse(self.filepath)
            self.root = self.tree.getroot()
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML file {self.filepath}: {e}")
            raise
        except FileNotFoundError:
            logger.error(f"XML file not found: {self.filepath}")
            raise

    def _find_text(self, path: str, default: Any = None) -> Any:
        """Helper to find text content of an element, with a default."""
        element = self.root.find(path)
        if element is not None and element.text:
            return element.text.strip()
        return default

    def summarize(self) -> Dict[str, Any]:
        """Gathers all key statistics into a single dictionary."""
        summary = {}

        # Basic Info
        summary["spacegroup"] = self._find_text("AutoProc/spaceGroup")
        summary["wavelength"] = self._find_text("AutoProc/wavelength")
        cell_a = self._find_text("AutoProc/refinedCell_a")
        cell_b = self._find_text("AutoProc/refinedCell_b")
        cell_c = self._find_text("AutoProc/refinedCell_c")
        cell_alpha = self._find_text("AutoProc/refinedCell_alpha")
        cell_beta = self._find_text("AutoProc/refinedCell_beta")
        cell_gamma = self._find_text("AutoProc/refinedCell_gamma")
        if all([cell_a, cell_b, cell_c, cell_alpha, cell_beta, cell_gamma]):
            summary["unitcell"] = (
                f"{cell_a} {cell_b} {cell_c} {cell_alpha} {cell_beta} {cell_gamma}"
            )

        # Scaling Statistics (Overall)
        overall_stats = self.root.find(
            ".//AutoProcScalingStatistics[scalingStatisticsType='overall']"
        )
        if overall_stats is not None:
            summary["highresolution"] = self._find_text_in_element(
                overall_stats, "resolutionLimitHigh"
            )
            summary["lowresolution"] = self._find_text_in_element(
                overall_stats, "resolutionLimitLow"
            )
            summary["rmerge"] = self._find_text_in_element(overall_stats, "rMerge")
            summary["rmeas"] = self._find_text_in_element(
                overall_stats, "rMeasAllIPlusIMinus"
            )
            summary["rpim"] = self._find_text_in_element(
                overall_stats, "rPimAllIPlusIMinus"
            )
            summary["isigmai"] = self._find_text_in_element(
                overall_stats, "meanIOverSigI"
            )
            summary["completeness"] = self._find_text_in_element(
                overall_stats, "completeness"
            )
            summary["multiplicity"] = self._find_text_in_element(
                overall_stats, "multiplicity"
            )
            summary["cchalf"] = self._find_text_in_element(overall_stats, "ccHalf")
            summary["anom_completeness"] = self._find_text_in_element(
                overall_stats, "anomalousCompleteness"
            )
            summary["anom_multiplicity"] = self._find_text_in_element(
                overall_stats, "anomalousMultiplicity"
            )
            summary["anom_cchalf"] = self._find_text_in_element(
                overall_stats, "ccAnomalous"
            )
            summary["Nobs"] = self._find_text_in_element(
                overall_stats, "nTotalObservations"
            )
            summary["Nuniq"] = self._find_text_in_element(
                overall_stats, "nTotalUniqueObservations"
            )

        # Scaling Statistics (Inner Shell)
        inner_stats = self.root.find(
            ".//AutoProcScalingStatistics[scalingStatisticsType='innerShell']"
        )
        if inner_stats is not None:
            summary["highresolution_inner"] = self._find_text_in_element(
                inner_stats, "resolutionLimitHigh"
            )
            summary["lowresolution_inner"] = self._find_text_in_element(
                inner_stats, "resolutionLimitLow"
            )
            summary["rmerge_inner"] = self._find_text_in_element(inner_stats, "rMerge")
            summary["rmeas_inner"] = self._find_text_in_element(
                inner_stats, "rMeasAllIPlusIMinus"
            )
            summary["rpim_inner"] = self._find_text_in_element(
                inner_stats, "rPimAllIPlusIMinus"
            )
            summary["isigmai_inner"] = self._find_text_in_element(
                inner_stats, "meanIOverSigI"
            )
            summary["completeness_inner"] = self._find_text_in_element(
                inner_stats, "completeness"
            )
            summary["multiplicity_inner"] = self._find_text_in_element(
                inner_stats, "multiplicity"
            )
            summary["cchalf_inner"] = self._find_text_in_element(inner_stats, "ccHalf")

        # Scaling Statistics (Outer Shell)
        outer_stats = self.root.find(
            ".//AutoProcScalingStatistics[scalingStatisticsType='outerShell']"
        )
        if outer_stats is not None:
            summary["highresolution_outer"] = self._find_text_in_element(
                outer_stats, "resolutionLimitHigh"
            )
            summary["lowresolution_outer"] = self._find_text_in_element(
                outer_stats, "resolutionLimitLow"
            )
            summary["rmerge_outer"] = self._find_text_in_element(outer_stats, "rMerge")
            summary["rmeas_outer"] = self._find_text_in_element(
                outer_stats, "rMeasAllIPlusIMinus"
            )
            summary["rpim_outer"] = self._find_text_in_element(
                outer_stats, "rPimAllIPlusIMinus"
            )
            summary["isigmai_outer"] = self._find_text_in_element(
                outer_stats, "meanIOverSigI"
            )
            summary["completeness_outer"] = self._find_text_in_element(
                outer_stats, "completeness"
            )
            summary["multiplicity_outer"] = self._find_text_in_element(
                outer_stats, "multiplicity"
            )
            summary["cchalf_outer"] = self._find_text_in_element(outer_stats, "ccHalf")

        # Attachments
        for attachment in self.root.findall(".//AutoProcProgramAttachment"):
            file_type = self._find_text_in_element(attachment, "fileType")
            file_name = self._find_text_in_element(attachment, "fileName")
            file_path = self._find_text_in_element(attachment, "filePath")

            if file_path and file_name:
                # Robustly determine the full path
                if file_path.endswith(file_name):
                    full_path = file_path
                else:
                    full_path = os.path.join(file_path, file_name)

                if file_name == "summary.html":
                    summary["report_url"] = full_path
                elif (
                    "truncate-unique.mtz" in file_name
                    or "staraniso" in file_name
                    and file_name.endswith(".mtz")
                ):
                    summary["truncate_mtz"] = full_path
                elif file_name.endswith(".table1"):
                    try:
                        with open(full_path, "r") as f:
                            summary["table1"] = f.read()
                    except Exception as e:
                        logger.warning(f"Could not read table1 file {full_path}: {e}")

        return summary

    def _find_text_in_element(self, element, tag, default=None):
        """Helper to find text in a sub-element."""
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return default


if __name__ == "__main__":
    a = AutoPROCXmlParser()
    s = a.summarize()
    print(s)
