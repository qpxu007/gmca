# ==============================================================================
# 4. DATABASE QUERIES
# ==============================================================================
# --- SQLAlchemy Imports ---
from sqlalchemy import desc, or_, and_, asc, cast, Float
import re

try:
    from qp2.log.logging_config import get_logger

    logger = get_logger(__name__)
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

from qp2.data_viewer.models import (
    DatasetRun,
    PipelineStatus,
    DataProcessResults,
    ScreenStrategyResults,
)


def _get_user_auth_filter(model, user):
    return or_(model.username == user.username, model.primary_group == user.primary_group)


def query_latest_dataset_run_id(db_session, user):
    """
    Performs a highly optimized query to get only the ID and creation time
    of the most recent dataset run visible to the user.
    """

    latest_run = db_session.query(
        DatasetRun.data_id,
        DatasetRun.created_at
    ).filter(
        DatasetRun.username == user.primary_group
    ).order_by(
        desc(DatasetRun.data_id)  # Order by ID is usually fastest on indexed primary key
    ).first()

    return latest_run  # Returns a single result object (or None)


def query_dataset_run(
        db_session, user, limit=500, search_text=None, sort_by="data_id", sort_order="desc"
):
    query = db_session.query(
        DatasetRun.data_id,
        DatasetRun.username,
        DatasetRun.run_prefix,
        DatasetRun.total_frames,
        DatasetRun.collect_type,
        DatasetRun.master_files,
        DatasetRun.headers,
        DatasetRun.mounted,
        DatasetRun.meta_user,
        DatasetRun.created_at,
    ).filter(DatasetRun.username == user.primary_group)
    if search_text:
        query = query.filter(
            or_(
                DatasetRun.master_files.like(f"%{search_text}%"),
                DatasetRun.run_prefix.like(f"%{search_text}%"),
                DatasetRun.collect_type.like(f"%{search_text}%"),
            )
        )
    # return query.order_by(desc(DatasetRun.created_at)).limit(limit).all()
    sort_column = getattr(DatasetRun, sort_by, DatasetRun.data_id)
    order_func = desc if sort_order == "desc" else asc
    return query.order_by(order_func(sort_column)).limit(limit).all()


def query_dataprocess(
        db_session,
        user,
        search_text=None,
        limit=500,
        start_date=None,
        sort_by="id",
        sort_order="desc",
):
    user_auth_filter = _get_user_auth_filter(PipelineStatus, user)
    query = db_session.query(
        PipelineStatus.id,
        PipelineStatus.sampleName.label("name"),
        PipelineStatus.pipeline,
        PipelineStatus.imageSet,
        PipelineStatus.state,
        DataProcessResults.isa,
        DataProcessResults.report_url.label("Summary"),
        DataProcessResults.wavelength.label("wav"),
        DataProcessResults.spacegroup.label("Symm"),
        DataProcessResults.unitcell.label("Cell"),
        DataProcessResults.highresolution.label("h_res"),
        DataProcessResults.rmerge.label("Rsym"),
        DataProcessResults.rmeas.label("Rmeas"),
        DataProcessResults.rpim.label("Rpim"),
        DataProcessResults.isigmai.label("IsigI"),
        DataProcessResults.multiplicity.label("multi"),
        DataProcessResults.completeness.label("Cmpl"),
        DataProcessResults.anom_completeness.label("a_Cmpl"),
        PipelineStatus.warning,
        PipelineStatus.logfile,
        DataProcessResults.table1,
        PipelineStatus.elapsedtime,
        PipelineStatus.imagedir,
        DataProcessResults.firstFrame,
        DataProcessResults.workdir,
        DataProcessResults.scale_log,
        DataProcessResults.truncate_log,
        DataProcessResults.truncate_mtz,
        DataProcessResults.run_stats,
        DataProcessResults.id.label("reprocess"),
        DataProcessResults.solve,
        PipelineStatus.id.label("delete"),
        # fields after this not displayed, for reprocessing use
        PipelineStatus.datasets,
        PipelineStatus.username,
        PipelineStatus.primary_group,
        PipelineStatus.esaf_id,
        PipelineStatus.pi_id,
    ).outerjoin(
        DataProcessResults, PipelineStatus.id == DataProcessResults.pipelinestatus_id
    )
    filters = [user_auth_filter, ~(PipelineStatus.pipeline.contains("_strategy"))]
    if start_date:
        filters.append(PipelineStatus.starttime >= start_date)
    
    # --- Column Filtering Logic ---
    remaining_search_text = search_text
    if search_text:
        # Map user-friendly names to actual Model columns
        # Note: DataProcessResults columns are Strings, so we will need to cast them for comparison
        col_map = {
            "ISa": DataProcessResults.isa,
            "isa": DataProcessResults.isa,
            "Rsym": DataProcessResults.rmerge,
            "rsym": DataProcessResults.rmerge,
            "Rmeas": DataProcessResults.rmeas,
            "rmeas": DataProcessResults.rmeas,
            "HighRes": DataProcessResults.highresolution,
            "h_res": DataProcessResults.highresolution,
            "highres": DataProcessResults.highresolution,
            "res": DataProcessResults.highresolution,
            "Cmpl": DataProcessResults.completeness,
            "cmpl": DataProcessResults.completeness,
            "completeness": DataProcessResults.completeness,
            "IsigI": DataProcessResults.isigmai,
            "isigi": DataProcessResults.isigmai,
            "Multi": DataProcessResults.multiplicity,
            "multi": DataProcessResults.multiplicity,
        }

        tokens = search_text.split()
        non_filter_tokens = []
        
        # Regex to capture: (Column)(Operator)(Value)
        # Operators: >=, <=, >, <, =, :
        pattern = re.compile(r"^([a-zA-Z0-9_]+)(>=|<=|>|<|=|:)(.+)$")

        for token in tokens:
            match = pattern.match(token)
            parsed = False
            if match:
                col_name, op, val_str = match.groups()
                if col_name in col_map:
                    try:
                        val = float(val_str)
                        db_col = col_map[col_name]
                        # Cast the string column to Float for numerical comparison
                        numeric_col = cast(db_col, Float)
                        
                        if op == ">":
                            filters.append(numeric_col > val)
                        elif op == "<":
                            filters.append(numeric_col < val)
                        elif op == ">=":
                            filters.append(numeric_col >= val)
                        elif op == "<=":
                            filters.append(numeric_col <= val)
                        elif op in ["=", ":"]:
                            filters.append(numeric_col == val)
                        
                        parsed = True
                    except ValueError:
                        # value wasn't a float, treat as normal text
                        pass
            
            if not parsed:
                non_filter_tokens.append(token)
        
        # Reconstruct search text without the consumed filter tokens
        remaining_search_text = " ".join(non_filter_tokens)

    # Use the remaining text for the generic LIKE search
    if remaining_search_text:
        filters.append(
            or_(
                PipelineStatus.sampleName.like(f"%{remaining_search_text}%"),
                PipelineStatus.pipeline.like(f"%{remaining_search_text}%"),
                PipelineStatus.imagedir.like(f"%{remaining_search_text}%"),
                PipelineStatus.state.like(f"%{remaining_search_text}%"),
            )
        )
    query = query.filter(and_(*filters))
    sort_column = getattr(PipelineStatus, sort_by, PipelineStatus.id)
    order_func = desc if sort_order == "desc" else asc
    # return query.order_by(desc(PipelineStatus.id)).limit(limit).all()
    return query.order_by(order_func(sort_column)).limit(limit).all()


def query_strategy(
        db_session,
        user,
        search_text=None,
        limit=500,
        start_date=None,
        sort_by="id",
        sort_order="desc",
):
    user_auth_filter = _get_user_auth_filter(PipelineStatus, user)
    query = db_session.query(
        PipelineStatus.id,
        PipelineStatus.sampleName.label("name"),
        PipelineStatus.pipeline,
        PipelineStatus.imageSet,
        PipelineStatus.state,
        ScreenStrategyResults.export2run.label("exp_strategy"),
        ScreenStrategyResults.bravais_lattice.label("lattice"),
        ScreenStrategyResults.unitcell.label("Cell"),
        ScreenStrategyResults.spacegroup.label("Symm"),
        ScreenStrategyResults.resolution_from_spots.label("h_res"),
        ScreenStrategyResults.mosaicity,
        ScreenStrategyResults.rmsd,
        ScreenStrategyResults.score,
        ScreenStrategyResults.n_spots,
        ScreenStrategyResults.osc_start,
        ScreenStrategyResults.osc_end,
        ScreenStrategyResults.osc_delta,
        ScreenStrategyResults.detectordistance.label("distance"),
        ScreenStrategyResults.completeness_native.label("cmpl"),
        ScreenStrategyResults.completeness_anomalous.label("a_cmpl"),
        ScreenStrategyResults.estimated_asu_content_aa.label("asu_aa"),
        ScreenStrategyResults.index_table,
        ScreenStrategyResults.xplanlog,
        ScreenStrategyResults.solvent_content,
        PipelineStatus.warning,
        PipelineStatus.logfile,
        PipelineStatus.elapsedtime,
        PipelineStatus.imagedir,
        ScreenStrategyResults.workdir,
        ScreenStrategyResults.sampleNumber.label("reprocess"),
        PipelineStatus.id.label("delete"),
        ScreenStrategyResults.pointgroup_choices.label("userChoose"),
        ScreenStrategyResults.anomalous,
        ScreenStrategyResults.referencedata,
        # fields after this not displayed, for reprocessing use
        PipelineStatus.datasets,
        PipelineStatus.username,
        PipelineStatus.primary_group,
        PipelineStatus.esaf_id,
        PipelineStatus.pi_id,
    ).outerjoin(
        ScreenStrategyResults,
        PipelineStatus.id == ScreenStrategyResults.pipelinestatus_id,
    )
    filters = [user_auth_filter, PipelineStatus.pipeline.contains("_strategy")]
    if start_date:
        filters.append(PipelineStatus.starttime >= start_date)
    if search_text:
        filters.append(
            or_(
                PipelineStatus.sampleName.like(f"%{search_text}%"),
                PipelineStatus.state.like(f"%{search_text}%"),
            )
        )
    sort_column = getattr(PipelineStatus, sort_by, PipelineStatus.id)
    order_func = desc if sort_order == "desc" else asc

    query = query.filter(and_(*filters))
    # return query.order_by(desc(PipelineStatus.id)).limit(limit).all()
    return query.order_by(order_func(sort_column)).limit(limit).all()


def delete_by_pid(db_session, pid):
    db_session.query(PipelineStatus).filter(PipelineStatus.id == pid).delete()
    db_session.commit()


def delete_by_pids(db_session, pids):
    if not pids:
        return
    db_session.query(PipelineStatus).filter(PipelineStatus.id.in_(pids)).delete(
        synchronize_session=False
    )
    db_session.commit()


if __name__ == "__main__":
    from qp2.xio.db_manager import DBManager

    dm = DBManager()

    with dm.get_session() as session:
        user = type("User", (object,), {"username": "staff", "primary_group": "staff"})
        results = query_dataset_run(session, user, search_text="mb")
        for result in results:
            logger.info(type(result))
            logger.info(int(result))
        results = query_dataprocess(session, user, search_text="test")
        for result in results:
            logger.info(type(result))

            logger.info(result)
