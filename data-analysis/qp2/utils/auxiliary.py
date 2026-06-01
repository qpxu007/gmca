#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  3 16:34:51 2016

@author: qxu
"""

import datetime
import json
import logging
import math
import multiprocessing as mp
import os
import re
import sys
import time
import traceback
from functools import wraps
from queue import Empty
from typing import List



logger = logging.getLogger(__name__)


def sanitize_unit_cell(cell_str):
    """Ensures unit cell string is space-separated and contains exactly 6 numbers."""
    if not cell_str:
        return None
        
    # Replace commas with spaces, then split into parts
    parts = cell_str.replace(",", " ").split()
    
    if len(parts) != 6:
        logger.warning(f"Invalid unit cell '{cell_str}': Expected 6 values, found {len(parts)}.")
        return None
        
    try:
        # Verify all parts are numbers
        [float(p) for p in parts]
    except ValueError:
        logger.warning(f"Invalid unit cell '{cell_str}': Found non-numeric values.")
        return None
        
    return " ".join(parts)


def sanitize_space_group(sg_str):
    """Sanitizes and validates a space group string from spreadsheet input.

    Removes parentheses, replaces underscores with empty string,
    and strips all whitespace to produce a compact symbol.
    Then validates against the known space group table.
    Returns None if the input is not a recognized space group.

    e.g. "P (21 21 21)" -> "P212121"
         "P_21_21_21"   -> "P212121"
         "F4(1)32"      -> "F4132"
         "invalid"      -> None
    """
    if not sg_str:
        return None
    s = str(sg_str).strip()
    if not s:
        return None
    # Remove parentheses and underscores, then collapse all whitespace
    s = s.replace("(", "").replace(")", "").replace("_", "")
    s = "".join(s.split())
    if not s:
        return None

    # Validate against known space groups
    from qp2.pipelines.gmcaproc.symmetry import Symmetry
    if Symmetry.symbol_to_number(s) is None:
        logger.warning(f"Unrecognized space group '{sg_str}' (sanitized: '{s}'). Discarding.")
        return None
    return s


def get_html_table(html_str, column_name=None):
    p = HTMLTableParser()
    p.feed(html_str)
    rows = p.tables[0]

    otable = []
    for row in rows[1:]:
        otable.append(dict(zip(rows[0], row)))

    if column_name:
        return [x[column_name] for x in otable]

    return otable


"""
from lxml import etree
def get_html_table(html_str, column_name=None):
    # must contain header
    table = etree.HTML(html_str).find("body/table")
    rows = iter(table)
    headers = [col.text for col in next(rows)]
    otable = []
    for row in rows:
        values = [col.text for col in row]
        otable.append(dict(zip(headers, values)))

    if column_name:
        return [x[column_name] for x in otable]

    return otable


# slow
def get_html_table(html_str, column_name=None):
    import pandas as pd

    table = pd.read_html(html_str)
    return table
"""

_number_rx = re.compile(r"[-+]?\d*\.\d+|[-+]?\d+")


def get_numbers(text_line: str) -> List[float]:
    if isinstance(text_line, (int, float)):
        return [float(text_line)]
    if not isinstance(text_line, str):
        return []
    return [float(x) for x in _number_rx.findall(text_line)]


def getNumbers(inStr):
    # extract all numbers in a string using regex [-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?
    # see https://docs.python.org/2/library/re.html#simulating-scanf
    # Return: a list of numbers
    import re

    numeric_const_pattern = r"""
    [-+]? # optional sign
    (?:
        (?:\d*\.\d+) # .1 .12 .123 etc 9.1 etc 98.1 etc
        |
        (?:\d+\.?)   # 1. 12. 123. etc 1 12 123 etc
    )
    # followed by optional exponent part if desired
    (?:[Ee][+-]?\d+)?
    """
    rx = re.compile(numeric_const_pattern, re.VERBOSE)
    return rx.findall(inStr)


def can_read_path(path):
    """
    Returns True if the current user can read the given file or directory, otherwise False.
    Handles both files and directories.
    """
    try:
        return os.access(path, os.R_OK)
    except Exception:
        return False


def isFloat(str):
    try:
        float(str)
        return True
    except ValueError:
        return False


def removeSpecialChars(inStr, charReplacement=""):
    # remove or replace special characters from a string
    import re, string

    rx = "[" + re.escape(string.punctuation) + "]"
    return re.sub(rx, charReplacement, inStr)


def getElbowPoint(curve):
    # given a list of numbers, calculate the position of the elbow point and quality
    # return: the index of the elbow point (0 based)
    #
    # The best solution was to find the point with the maximum distance d
    # to the line defined by the first and last point
    import numpy as np

    points = []
    for i in range(len(curve)):
        points.append(np.array([i, curve[i]]))

    vector = points[-1] - points[0]
    vectorNormalized = vector / np.linalg.norm(vector)
    # logger.debug(np.linalg.norm(vectorNormalized))

    distances = []
    for point in points:
        vectp = point - points[0]
        vect_dist = vectp - np.dot(vectp, vectorNormalized) * vectorNormalized
        distances.append(np.linalg.norm(vect_dist))
    return np.argmax(distances)


def dict2json(dict):
    return json.dumps(dict, sort_keys=True, indent=4)


def splitFileName(fname, fPattern=r"""(\d{3,6}\.|\.\d{3,6})"""):
    ## given a filename, split into prefix, seq, extension
    import re

    rx = re.compile(fPattern, re.VERBOSE)
    return re.split(rx, fname)


def get_resolution_bins(max_resolution, bins=10, rounding=2):
    """given maximum resolution, output resol shells in equal spacing if 1/reso^2"""
    one_over_res2 = 1.0 / max_resolution ** 2
    spacing = one_over_res2 / bins
    bounds = [(i + 1) * spacing for i in range(bins)]
    return [round(1.0 / math.sqrt(b), rounding) for b in bounds]


def check_run(wdir, input_files=[], out_files=[]):
    """a decorator function that can be used to check for existence of files
    before and after the run of a function"""

    def all_files_exist(wdir, files, fn):
        if not os.path.isdir(wdir):
            logger.error("directory {} does not exist.".format(wdir))

        if not all(os.path.isfile(os.path.join(wdir, f)) for f in files):
            logger.error("missing files {} in func {}.".format(files, fn))

        return True

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # before the run
            fn = func.func_name
            results = None
            if all_files_exist(wdir, input_files, fn):
                results = func(*args, **kwargs)

            # after the run
            if all_files_exist(wdir, out_files, fn):
                return results

        return wrapper

    return decorator


@check_run(
    wdir="/home/qxu/work/quickProcess",
    input_files=["require.py"],
    out_files=["XDS.INP"],
)
def index():
    pass


def rename_files_with_label(wdir, filenames, prefix="", label=None):
    # rename a list of files with a label with filename of a user defined label
    if not label:
        label = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    for fn in filenames:
        base, ext = os.path.splitext(fn)
        fin = os.path.join(wdir, fn)
        fout = os.path.join(wdir, prefix + base + label + ext)
        os.rename(fin, fout)


def timeit(func):
    wraps(func)

    def wrap(*args, **kwargs):
        startTime = time.time()
        result = func(*args, **kwargs)
        elapsedTime = 1.0 * (time.time() - startTime)
        # logger.info('function [{}] finished in {} {}'.format(func.__name__, elapsedTime, 's'))
        print("function [{}] finished in {} {}".format(func.__name__, elapsedTime, "s"))
        return result

    return wrap


def does_h5_frame_exist(h5master_file, frame_no=1):
    # print("---", h5master_file, frame_no)
    import fabio
    try:
        image = fabio.open(h5master_file, frame_no - 1)
        # h5.get_frame(frame_no)
    except Exception:
        return False
    return True  # able to extract the frame


def wait_until_files_exist(
        filenames, frame_no=None, interval=1, timeout=600, fail=False
):
    # check whether a file or files existed, wait until timeout is reached
    # if the filenames given is a hdf5 master file, and frame_no is also given check
    # existence of frame in the hdf5 data file as well
    sleeped = 0
    while True:
        if isinstance(filenames, (list, tuple)):
            exists = all(map(os.path.isfile, filenames))
        else:  # single cbf or h5 master
            exists = os.path.isfile(filenames)
            if filenames.endswith("h5") and frame_no:  # hdf5
                exists = does_h5_frame_exist(filenames, frame_no)
                # print("===", filenames, os.path.isfile(filenames), exists, frame_no)

        if exists:
            time.sleep(2)  # wait a few more sec, just in case
            return 0
        else:
            time.sleep(interval)
            sleeped += interval
            if sleeped >= timeout:
                logger.error(
                    "{} does not exist after timeout reached.".format(filenames)
                )
                if fail:
                    logger.error("images waited for not found, time out.")
                    raise SystemExit(1)
                else:
                    logger.warning(
                        "images waited for not found, continue with what we get."
                    )
                    return 2  # continue with whatever is present
            else:
                if sleeped % 10 == 0:
                    logger.warning("waiting for {}.".format(filenames))


class ExceptionWrapper(Exception):
    pass


def parallelize(func, data_list, nproc=4):
    # this function parallelize evaluation of [func(data) for data in data_list]
    def worker(data, pos, queue, semaphore):
        try:
            result = func(data)
            queue.put((pos, result))
        except Exception as e:
            t, v, tb = sys.exc_info()
            e = ExceptionWrapper()
            e.tb = traceback.format_tb(tb)
            # logger.error(tb)
            queue.put(pos, e)
        finally:
            semaphore.release()
        return

    plimit = mp.BoundedSemaphore(nproc)
    out_q = mp.Queue()

    jobs = list()
    for i, data in enumerate(data_list):
        plimit.acquire()
        p = mp.Process(target=worker, args=(data, i, out_q, plimit))
        p.start()
        jobs.append(p)

    results = list()
    while True:
        try:
            res = out_q.get_nowait()
        except Empty:
            time.sleep(0.2)
        else:
            if isinstance(res[1], ExceptionWrapper):
                logger.error("An error occurred for data: {}".format(res))
            else:
                results.append(res)  # or yield data

        if not any(j.is_alive() for j in jobs) and out_q.empty():
            break  # all the workers are done and nothing is in the queue

            # print("<<< {} {} {}".format(results, out_q.empty(),[j.is_alive() for j in jobs]))
    # print(">>> results = {}".format(results))

    return [r[1] for r in sorted(results)]


def get_latest_collect_directory(root_directory=None, followlinks=True):
    """find the most recently created directory with at least one cbf file"""
    DNAME_PATTERN = r"""(23ID[B|D]_\d{4}_\d{2}_\d{2})"""
    if not root_directory:
        root_directory = os.getenv("HOME")

    # file = max(glob.iglob('{}/**/{}'.format(root_directory, '*.cbf'), recursive=True), key=os.path.getctime)
    roots = []
    for root, dirs, files in os.walk(root_directory, followlinks=followlinks):
        if re.search(DNAME_PATTERN, root):
            if all(k not in root for k in ["_strategy", "_fast_dp", "_GMCAproc"]):
                ncbfs = [f.endswith(".cbf") for f in files]
                if ncbfs and sum(ncbfs) >= 1:
                    roots.append(root)
    return max(roots, key=os.path.getctime) if roots else None


def get_directories_contains_file(
        filename,
        root_directory=None,
        sort_by_ctime=True,
        followlinks=True,
        ftype="filename",
):
    """return all directories containing filename"""
    if not root_directory:
        root_directory = os.getenv("HOME")

    paths = []
    for root, dirs, files in os.walk(root_directory, followlinks=followlinks):
        if ftype == "filename":
            for file in files:
                if re.compile(filename).match(file):
                    paths.append(root)
                    break
        else:
            for dirn in dirs:
                if re.compile(filename).match(dirn):
                    paths.append(root)
                    break

    if sort_by_ctime:
        paths = sorted(paths, key=os.path.getctime)
    return paths


def rel_path(refpath, file_or_directory):
    """return the relate path for file_or_dir from refpath--usually current working directory"""
    refdir = refpath
    if os.path.isfile(refpath):
        refdir = os.path.dirname(refpath)

    refdir = os.path.abspath(refdir)
    target = os.path.abspath(file_or_directory)

    return os.path.relpath(target, refdir)


def shorter_path(refpath, file_or_directory):
    """return abs path or relative path, whichever is shorter in length"""
    relpath = rel_path(refpath, file_or_directory)

    pathout = file_or_directory

    if len(file_or_directory) > len(relpath):
        pathout = relpath

    if len(pathout) > 80:
        print(
            "Warning: length of {} > 80 chars, which may be too long.".format(pathout)
        )

    return pathout


def reject_outliers(data, m=2, left_tail=False):
    import numpy as np

    if isinstance(data, list):
        data = np.array(data)
    if left_tail:
        return data[np.mean(data) - data < m * np.std(data)]
    else:
        return data[abs(data - np.mean(data)) < m * np.std(data)]


def get_outliers(data, m=2, left_tail=False, indices=False):
    # m: sigma level
    # left_tail: if true, only reject left tail outliers
    # indices: if true, return indices instead of values of outliers
    import numpy as np

    if isinstance(data, list):
        data = np.array(data)

    if left_tail:
        if not indices:
            return data[np.mean(data) - data > m * np.std(data)]
        else:
            return (np.mean(data) - data > m * np.std(data)).nonzero()[0].tolist()
    else:
        if not indices:
            return data[abs(data - np.mean(data)) > m * np.std(data)]
        else:
            return (abs(data - np.mean(data)) > m * np.std(data)).nonzero()[0].tolist()


def reject_outliers2(data, m=3.5):
    import numpy as np

    if isinstance(data, list):
        data = np.array(data)

    d = data - np.median(data)
    mdev = np.median(np.abs(d))
    s = d / mdev if mdev else 0.0
    return data[s < m]


# -----------------------------------------------------------------------------
# Name:        html_table_parser
# Purpose:     Simple class for parsing an (x)html string to extract tables.
#              Written in python3
#
# Author:      Josua Schmid
#
# Created:     05.03.2014
# Copyright:   (c) Josua Schmid 2014
# Licence:     AGPLv3
# -----------------------------------------------------------------------------

from html.parser import HTMLParser


class HTMLTableParser(HTMLParser):
    """This class serves as a html table parser. It is able to parse multiple
    tables which you feed in. You can access the result per .tables field.
    """

    def __init__(
            self,
            decode_html_entities=False,
            data_separator=" ",
    ):

        HTMLParser.__init__(self)

        self._parse_html_entities = decode_html_entities
        self._data_separator = data_separator

        self._in_td = False
        self._in_th = False
        self._current_table = []
        self._current_row = []
        self._current_cell = []
        self.tables = []

    def handle_starttag(self, tag, attrs):
        """We need to remember the opening point for the content of interest.
        The other tags (<table>, <tr>) are only handled at the closing point.
        """
        if tag == "td":
            self._in_td = True
        if tag == "th":
            self._in_th = True

    def handle_data(self, data):
        """This is where we save content to a cell"""
        if self._in_td or self._in_th:
            self._current_cell.append(data.strip())

    def handle_charref(self, name):
        """Handle HTML encoded characters"""

        if self._parse_html_entities:
            self.handle_data(self.unescape("&#{};".format(name)))

    def handle_endtag(self, tag):
        """Here we exit the tags. If the closing tag is </tr>, we know that we
        can save our currently parsed cells to the current table as a row and
        prepare for a new row. If the closing tag is </table>, we save the
        current table and prepare for a new one.
        """
        if tag == "td":
            self._in_td = False
        elif tag == "th":
            self._in_th = False

        if tag in ["td", "th"]:
            final_cell = self._data_separator.join(self._current_cell).strip()
            self._current_row.append(final_cell)
            self._current_cell = []
        elif tag == "tr":
            self._current_table.append(self._current_row)
            self._current_row = []
        elif tag == "table":
            self.tables.append(self._current_table)
            self._current_table = []


if __name__ == "__main__":

    def time2(x):
        print(x * 2)


    # print(parallelize(time2, [1], nproc=1))

    # print(get_latest_collect_directory())
    # print(get_directories_contains_file("aimless.log"))

    html = "/mnt/beegfs/qxu/23BM_2017_08_08/14site-inv/collect/vector-small/process/repo/kamo_xds/report.html"
    print(get_html_table(open(html).read()))
    print(get_html_table(open(html).read(), column_name="Resn (Å)"))
    print(get_html_table(open(html).read(), column_name="Dataset"))
    print(get_html_table(open(html).read(), column_name="Cmpl (%)"))


    def estimate_best_resolution(file, default=3.0, pad=0.5):
        if os.path.exists(file):
            html = open(file).read()
            res = get_html_table(html, column_name="Resn (Å)")
            if res:
                res = [float(x) for x in res if x.replace(".", "", 1).isdigit()]
                return round(min(res) - pad, 1) if res else default

        return default


    print("max res=", estimate_best_resolution(html))


    def estimate_mean_completeness(file, default=3):
        # mean completeness per dataset
        if os.path.exists(file):
            with open(file) as fh:
                html = fh.read()
                c = get_html_table(html, column_name="Cmpl (%)")
                print("res=", c)
                if c:
                    cmpl = [float(x) for x in c if x.replace(".", "", 1).isdigit()]
                    print("cmpl=", cmpl)
                    if cmpl:
                        return sum(cmpl) / len(cmpl)

        return default


    print("avg cmpl=", estimate_mean_completeness(html))

    exit()

    # print(relpath('./w', './work/x.pdf'));
    print(
        rel_path(
            "/home/qxu/work/mosflm.py",
            "/home/qxu/work/js/Select-1.2.2/js/dataTables.select.js",
        )
    )
    print(rel_path("/home/qxu/work/mosflm.py", "/tmp"))
    print(shorter_path("/tmp/x", "/tmp"))
    print("outlier testing")
    a = [-3, 1, 2, 3, 4, 5, 6, 10, -5, 3.0, -20]
    print(reject_outliers(a, m=1.0, left_tail=True))
    # print(reject_outliers2(a, m=2))
    print(a)
    print(get_outliers(a, m=1.0, left_tail=False))
    print(get_outliers(a, m=1.0, left_tail=False, indices=True))
