import json
import os


def clean_path(value):
    # 1. Isolate the path part by removing the scheme, if present.
    path_part = value.replace("file://", "")

    # 2. Force the path to be treated as a local absolute path by removing
    #    all leading slashes and then adding a single one back. This corrects
    #    inputs like '//mnt/...' to be '/mnt/...'.
    clean_path = "/" + path_part.lstrip("/")

    # 3. Normalize the rest of the path to handle other inconsistencies like '//'
    normalized_path = os.path.normpath(clean_path)
    return normalized_path


def render_as_link(value):
    """
    Renders a file path as a clickable HTML link.
    This version robustly cleans and normalizes the path to create a valid URL.
    """
    if not value or not isinstance(value, str):
        return ""

    normalized_path = clean_path(value)

    # 4. Rebuild a standard, clean href for use in HTML.
    href = f"file://{normalized_path}"

    display_text = "[Open]"
    return (
        f'<p align="center" style="margin:0;"><a href="{href}">{display_text}</a></p>'
    )


def render_summary_links(row):
    links = []
    report_url = getattr(row, "Summary", None)
    solve_dir = getattr(row, "solve", None)

    if report_url:
        localpath = clean_path(report_url)
        href = f"file://{localpath}"
        links.append(f"<a href='{href}' title='report'>Report</a>")

    if solve_dir:
        links.append(f"<a href='coot:{solve_dir}' title='Run Coot'>Map</a>")

    # Wrap the final output in a centered paragraph tag
    html_content = " | ".join(links)
    return (
        f'<p align="center" style="margin:0;">{html_content}</p>'
        if html_content
        else ""
    )


def render_view_button(value):
    """Renders a '[View]' link if the cell has content."""
    if value and str(value).strip():
        # Use a custom scheme to trigger the dialog in the delegate
        return f'<p align="center" style="margin:0;"><a href="view_preformatted:">[View]</a></p>'
    return ""


def render_export_button(row):
    """
    Renders an 'Export' link only if the 'osc_start' column has a value.
    """
    # Get the value from the 'osc_start' column.
    osc_start_value = getattr(row, "osc_start", None)
    pipelinestatus_id = getattr(row, "id", None)

    # --- MODIFIED LOGIC ---
    # Check if an ID exists AND if osc_start_value is not None and not an empty string.
    if (
            pipelinestatus_id is not None
            and osc_start_value is not None
            and str(osc_start_value).strip()
    ):
        return f'<p align="center" style="margin:0;"><a href="export_strategy:{pipelinestatus_id}">Export</a></p>'

    # For all other cases, return an empty string.
    return ""


def render_master_files_summary(json_string):
    """
    Parses a JSON string and returns a summary showing the total file count
    and the basename of the first file.
    """
    if not json_string or not isinstance(json_string, str):
        return ""

    try:
        files = json.loads(json_string)
        if not isinstance(files, list):
            return "[Invalid Format]"

        file_count = len(files)
        if file_count == 0:
            return "[No files]"

        # Get just the filename of the first file in the list
        first_basename = os.path.basename(files[0]) if files[0] else ""

        return f'<p align="center" style="margin:0;">{first_basename} [{file_count} files]</p>'

    except json.JSONDecodeError:
        return f'<p align="center" style="margin:0;">[Invalid JSON] {json_string[:30]}...</p>'


def render_metadata_summary(json_string):
    """
    Parses a JSON string. In the table, it displays a summary of the
    contents and a '[View]' link to open a dialog with the
    fully formatted data.
    """
    if not json_string or not str(json_string).strip():
        return ""

    summary = ""
    try:
        # Attempt to parse the JSON to create a summary
        data = json.loads(json_string)
        if isinstance(data, list):
            summary = f"[{len(data)} items]"
        elif isinstance(data, dict):
            summary = f"[{len(data)} keys]"
        else:
            # Fallback for valid JSON that isn't a list or dict
            summary = "[Data]"

    except (json.JSONDecodeError, TypeError):
        # If the string is not valid JSON, the summary will be empty,
        # but we can still offer a button to view the raw text.
        summary = "[Raw Text]"

    # The link uses the 'view_preformatted' scheme to trigger the existing dialog logic
    view_link = '<a href="view_preformatted:">[View]</a>'

    # Return an HTML paragraph containing the summary and the view link
    return f'<p align="center" style="margin:0;">{summary} {view_link}</p>'


def render_choices_menu(value):
    """
    Renders a semicolon-separated string as a clickable button.
    The first choice is displayed as the button text. Clicking it will
    trigger a pop-up menu with all choices.
    """
    if not value or not str(value).strip():
        return ""

    choices = [v.strip() for v in value.split(";") if v.strip()]
    if not choices:
        return ""

    # Display the first choice as the button's text, with a dropdown arrow symbol
    display_text = f"{choices[0].split()[0]} ▼"

    # The href uses a custom 'show_choices' scheme and contains all the data.
    # We replace semicolons with a safe character like '|' for the href attribute.
    href_data = "|".join(choices)

    return f'<p align="center" style="margin:0;"><a href="show_choices:{href_data}">{display_text}</a></p>'


def render_delete_button(row):
    """
    Renders a '[Delete]' link with the row's primary ID.
    """
    # The 'id' key is the primary key for PipelineStatus
    pipelinestatus_id = getattr(row, "id", None)

    if pipelinestatus_id is not None:
        # Use a custom scheme to trigger the delete action in the delegate
        return f'<p align="center" style="margin:0;"><a href="delete_entry:{pipelinestatus_id}">[Delete]</a></p>'
    return ""


TAB_CONFIG = {
    "Datasets": {
        "query_func_name": "query_dataset_run",
        "columns": [
            {"key": "data_id", "display": "ID", "visible": False, "priority": 1},
            {"key": "username", "display": "User", "visible": True, "priority": 2},
            {
                "key": "run_prefix",
                "display": "Run Prefix",
                "visible": True,
                "priority": 1,
            },
            {
                "key": "total_frames",
                "display": "Frames",
                "visible": True,
                "priority": 1,
            },
            {
                "key": "collect_type",
                "display": "Collection Type",
                "visible": True,
                "priority": 1,
            },
            {
                "key": "master_files",
                "display": "Masterfiles",
                "visible": False,
                "priority": 2,
                "renderer": render_master_files_summary,
            },
            {
                "key": "headers",
                "display": "Metadata",
                "visible": True,
                "priority": 1,
                "renderer": render_metadata_summary,
            },
            {
                "key": "mounted",
                "display": "Mounted",
                "visible": False,
                "priority": 2,
            },
            {
                "key": "meta_user",
                "display": "Spreadsheet",
                "visible": False,
                "priority": 2,
                "renderer": render_metadata_summary,
            },
            {"key": "created_at", "display": "Created", "visible": True, "priority": 3},
        ],
    },
    "Processing": {
        "query_func_name": "query_dataprocess",
        "columns": [
            {"key": "id", "display": "ID", "visible": False, "priority": 2},
            {"key": "name", "display": "Sample", "visible": True, "priority": 1},
            {"key": "pipeline", "display": "Pipeline", "visible": True, "priority": 1},
            {"key": "imageSet", "display": "Image Set", "visible": True, "priority": 1},
            {
                "key": "state",
                "display": "State",
                "visible": True,
                "priority": 1,
                "renderer": "state_renderer",
            },
            {
                "key": "Summary",
                "display": "Report",
                "visible": True,
                "priority": 1,
                "renderer": render_summary_links,
                "renderer_uses_row": True,
            },
            {
                "key": "isa",
                "display": "ISa",
                "visible": False,
                "priority": 1,
            },
            {"key": "wav", "display": "Wavelength", "visible": False, "priority": 3},
            {"key": "Symm", "display": "Space Group", "visible": True, "priority": 1},
            {"key": "Cell", "display": "Unit Cell", "visible": False, "priority": 2},
            {"key": "h_res", "display": "Res.", "visible": True, "priority": 1},
            {"key": "Rsym", "display": "Rsym", "visible": True, "priority": 1},
            {"key": "Rmeas", "display": "Rmeas", "visible": False, "priority": 2},
            {"key": "Rpim", "display": "Rpim", "visible": False, "priority": 2},
            {"key": "IsigI", "display": "I/sig(I)", "visible": True, "priority": 1},
            {"key": "multi", "display": "Mult.", "visible": True, "priority": 1},
            {"key": "Cmpl", "display": "Compl. %", "visible": True, "priority": 1},
            {
                "key": "a_Cmpl",
                "display": "Anom. Compl. %",
                "visible": False,
                "priority": 2,
            },
            {"key": "warning", "display": "Warning", "visible": False, "priority": 3},
            {
                "key": "logfile",
                "display": "Log File",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "table1",
                "display": "Table1",
                "visible": True,
                "priority": 1,
                "renderer": render_view_button,
            },
            {"key": "elapsedtime", "display": "Time", "visible": False, "priority": 3},
            {
                "key": "imagedir",
                "display": "Image Dir",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "firstFrame",
                "display": "Start Frame",
                "visible": False,
                "priority": 3,
            },
            {
                "key": "workdir",
                "display": "Work Dir",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "scale_log",
                "display": "Scale Log",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "truncate_log",
                "display": "Truncate Log",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "truncate_mtz",
                "display": "MTZ File",
                "visible": True,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "run_stats",
                "display": "Run Stats",
                "visible": False,
                "priority": 3,
            },
            {
                "key": "reprocess",
                "display": "Reprocess ID",
                "visible": False,
                "priority": 3,
            },
            {
                "key": "solve",
                "display": "Solve",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "delete",
                "display": "Delete",
                "visible": False,
                "renderer": render_delete_button,
                "renderer_uses_row": True,
                "priority": 3,
            },
        ],
    },
    "Strategy": {
        "query_func_name": "query_strategy",
        "columns": [
            # The key must match the label/name from the query function
            {"key": "id", "display": "ID", "visible": False, "priority": 2},
            {"key": "name", "display": "Sample", "visible": True, "priority": 1},
            {"key": "pipeline", "display": "Pipeline", "visible": True, "priority": 1},
            {"key": "imageSet", "display": "Image Set", "visible": True, "priority": 1},
            {
                "key": "state",
                "display": "State",
                "visible": True,
                "priority": 1,
                "renderer": "state_renderer",
            },
            {
                "key": "exp_strategy",
                "display": "Exp. Strategy",
                "visible": True,
                "priority": 1,
                "renderer": render_export_button,
                "renderer_uses_row": True,
            },
            {"key": "lattice", "display": "Lattice", "visible": False, "priority": 2},
            {"key": "Cell", "display": "Cell", "visible": False, "priority": 2},
            {"key": "Symm", "display": "Space Group", "visible": True, "priority": 1},
            {"key": "h_res", "display": "Res.", "visible": True, "priority": 1},
            {
                "key": "mosaicity",
                "display": "Mosaicity",
                "visible": False,
                "priority": 2,
            },
            {"key": "rmsd", "display": "RMSD", "visible": False, "priority": 2},
            {"key": "score", "display": "Score", "visible": True, "priority": 1},
            {"key": "n_spots", "display": "# Spots", "visible": False, "priority": 2},
            {
                "key": "osc_start",
                "display": "Start (°)",
                "visible": True,
                "priority": 1,
            },
            {"key": "osc_end", "display": "End (°)", "visible": True, "priority": 1},
            {
                "key": "osc_delta",
                "display": "Delta (°)",
                "visible": False,
                "priority": 2,
            },
            {"key": "distance", "display": "Distance", "visible": False, "priority": 2},
            {"key": "cmpl", "display": "Compl. %", "visible": False, "priority": 2},
            {
                "key": "a_cmpl",
                "display": "Anom. Compl. %",
                "visible": False,
                "priority": 2,
            },
            {"key": "asu_aa", "display": "ASU (a.a.)", "visible": True, "priority": 1},
            {
                "key": "index_table",
                "display": "Index Table",
                "visible": False,
                "priority": 3,
                "renderer": render_view_button,
            },
            {
                "key": "xplanlog",
                "display": "XPlan Log",
                "visible": False,
                "priority": 3,
                "renderer": render_view_button,
            },
            {
                "key": "solvent_content",
                "display": "Solvent %",
                "visible": False,
                "priority": 3,
            },
            {"key": "warning", "display": "Warning", "visible": False, "priority": 3},
            {
                "key": "logfile",
                "display": "Log File",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {"key": "elapsedtime", "display": "Time", "visible": False, "priority": 3},
            {
                "key": "imagedir",
                "display": "Image Dir",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "workdir",
                "display": "Work Dir",
                "visible": False,
                "priority": 3,
                "renderer": render_as_link,
            },
            {
                "key": "reprocess",
                "display": "Reprocess ID",
                "visible": False,
                "priority": 3,
            },
            {
                "key": "delete",
                "display": "Delete",
                "visible": False,
                "renderer": render_delete_button,
                "renderer_uses_row": True,
                "priority": 3,
            },
            {
                "key": "userChoose",
                "display": "Point Group Choices",
                "visible": False,
                "priority": 3,
                "renderer": render_choices_menu,
            },
            {
                "key": "anomalous",
                "display": "Anomalous",
                "visible": False,
                "priority": 3,
            },
            {
                "key": "referencedata",
                "display": "Reference Data",
                "visible": False,
                "priority": 3,
            },
        ],
    },
}
