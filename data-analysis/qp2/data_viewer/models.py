# ==============================================================================
# 1. DATABASE MODELS
# ==============================================================================
import os
import socket
from datetime import datetime
from typing import List

# --- SQLAlchemy Imports ---
from sqlalchemy import (
    DateTime,
    Date,
    Boolean,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


# Base = declarative_base()
class Base(DeclarativeBase):
    pass


class DatasetRun(Base):
    __tablename__ = "dataset_runs"
    __table_args__ = {"extend_existing": True}

    data_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(255), nullable=True)
    run_prefix: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    collect_type: Mapped[str] = mapped_column(String(255), nullable=True)
    master_files: Mapped[str] = mapped_column(Text(16777215), nullable=True)
    total_frames: Mapped[int] = mapped_column(nullable=True)
    headers: Mapped[str] = mapped_column(Text(16777215), nullable=True)
    mounted: Mapped[str] = mapped_column(String(255), nullable=True)
    meta_user: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    pipeline_statuses: Mapped[List["PipelineStatus"]] = relationship(
        "PipelineStatus", backref="dataset_run"
    )

    __table_args__ = (
        UniqueConstraint("run_prefix", "created_at", name="uq_run_prefix_created_at"),
    )


class PipelineStatus(Base):
    __tablename__ = "pipelinestatus"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    command: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    state: Mapped[str] = mapped_column(String(20), nullable=False)
    pipeline: Mapped[str] = mapped_column(
        String(20), nullable=False, default="gmcaproc", index=True
    )
    imagedir: Mapped[str] = mapped_column(String(255), nullable=False)
    workdir: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    log: Mapped[str] = mapped_column(Text, nullable=False, default="")
    warning: Mapped[str] = mapped_column(Text, nullable=False, default="")
    processing_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hostname: Mapped[str] = mapped_column(
        String(255), nullable=False, default=socket.gethostname
    )
    sampleName: Mapped[str] = mapped_column(String(255), nullable=False)
    sampleNumber: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    username: Mapped[str] = mapped_column(
        String(30), nullable=False, default=lambda: os.getenv("USER"), index=True
    )
    beamline: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    starttime: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    elapsedtime: Mapped[str] = mapped_column(String(255), nullable=False, default="0s")
    imageSet: Mapped[str] = mapped_column(String(255), nullable=False, default="All")
    logfile: Mapped[str] = mapped_column(String(1024), nullable=False)
    delete: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pi_id: Mapped[int] = mapped_column(Integer, nullable=True)
    esaf_id: Mapped[int] = mapped_column(Integer, nullable=True)
    primary_group: Mapped[str] = mapped_column(String(30), nullable=True)
    datasets: Mapped[str] = mapped_column(Text, nullable=True)
    run_prefix: Mapped[str] = mapped_column(String(255), nullable=True)

    dataset_run_id: Mapped[int] = mapped_column(
        ForeignKey("dataset_runs.data_id"), nullable=True, index=True
    )

    dataproc_items: Mapped[List["DataProcessResults"]] = relationship(
        "DataProcessResults", backref="dataproc"
    )

    # We'll do the same for strategy for consistency, using a backref name that does not
    # collide with the 'strategy' column on ScreenStrategyResults
    strategy_items: Mapped[List["ScreenStrategyResults"]] = relationship(
        "ScreenStrategyResults", backref="strategy_status"
    )


class DataProcessResults(Base):
    __tablename__ = "dataprocessresults"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    pipelinestatus_id: Mapped[int] = mapped_column(
        ForeignKey("pipelinestatus.id", ondelete="CASCADE"), nullable=False
    )
    sampleNumber: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sampleName: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    imageSet: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    state: Mapped[str] = mapped_column(String(255), nullable=False, default="PENDING")
    software: Mapped[str] = mapped_column(String(255), nullable=False, default="XDS")
    scalingsoftware: Mapped[str] = mapped_column(
        String(255), nullable=False, default="XDS"
    )
    collectType: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    overlap: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inverseOn: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    firstFrame: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    workdir: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    subdir: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    imagedir: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    images: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    unitcell: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    lowresolution: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    lowresolution_inner: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    lowresolution_outer: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    highresolution: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    highresolution_inner: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    highresolution_outer: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    rmerge: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rmerge_inner: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rmerge_outer: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rpim: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rpim_inner: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rpim_outer: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    cchalf: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    cchalf_inner: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    cchalf_outer: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    completeness: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    completeness_inner: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    completeness_outer: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    anom_completeness: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    anom_completeness_inner: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    anom_completeness_outer: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    multiplicity: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    multiplicity_inner: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    multiplicity_outer: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    anom_multiplicity: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    anom_multiplicity_inner: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    anom_multiplicity_outer: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    isigmai: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    isigmai_inner: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    isigmai_outer: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    spacegroup: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    truncate_log: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    truncate_mtz: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    run_stats: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    log: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    warning: Mapped[str] = mapped_column(Text, nullable=False, default="")
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=func.now()
    )
    scale_log: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    start: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    end: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    prefix: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    isa: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rmeas: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rmeas_inner: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rmeas_outer: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    pipeline: Mapped[str] = mapped_column(
        String(255), nullable=False, default="gmcaproc"
    )
    wavelength: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    nobs: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    nuniq: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    table1: Mapped[str] = mapped_column(Text, nullable=False, default="")
    report_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    reprocess: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    solve: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    spacegroup_choices: Mapped[str] = mapped_column(Text, nullable=False, default="")


class ScreenStrategyResults(Base):
    __tablename__ = "screenstrategyresults"
    __table_args__ = {"extend_existing": True}

    sampleNumber: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    sampleName: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    directory: Mapped[str] = mapped_column(String(250), nullable=False, default="")
    images: Mapped[str] = mapped_column(String(250), nullable=False, default="")
    software: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    state: Mapped[str] = mapped_column(String(255), nullable=False, default="SPOT")
    workdir: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    index_table: Mapped[str] = mapped_column(Text, nullable=False, default="")
    unitcell: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    bravais_lattice: Mapped[str] = mapped_column(String(10), nullable=False, default="")
    rmsd: Mapped[str] = mapped_column(String(50), nullable=False, default="")
    ice_rings: Mapped[str] = mapped_column(String(50), nullable=False, default="")
    resolution_from_spots: Mapped[str] = mapped_column(
        String(30), nullable=False, default=""
    )
    n_spots: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    n_spots_ice: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    n_ice_rings: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    avg_spotsize: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    spacegroup: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    solution_number: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    penalty: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    mosaicity: Mapped[str] = mapped_column(String(50), nullable=False, default="")
    score: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    resolution_from_integ: Mapped[str] = mapped_column(
        String(30), nullable=False, default=""
    )
    warning: Mapped[str] = mapped_column(Text, nullable=False, default="")
    anomalous: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    osc_start: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    osc_end: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    osc_delta: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    completeness_native: Mapped[str] = mapped_column(
        String(30), nullable=False, default=""
    )
    completeness_anomalous: Mapped[str] = mapped_column(
        String(30), nullable=False, default=""
    )
    completeness_referencedata: Mapped[str] = mapped_column(
        String(30), nullable=False, default=""
    )
    detectorwarning: Mapped[str] = mapped_column(
        String(512), nullable=False, default=""
    )
    detectordistance: Mapped[str] = mapped_column(
        String(30), nullable=False, default="350.0"
    )
    referencedata: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    displaytext: Mapped[str] = mapped_column(String(250), nullable=False, default="")
    xplanlog: Mapped[str] = mapped_column(Text, nullable=False, default="")
    strategy: Mapped[str] = mapped_column(Text, nullable=False, default="")
    pipelinestatus_id: Mapped[int] = mapped_column(
        ForeignKey("pipelinestatus.id", ondelete="CASCADE"), nullable=False, index=True
    )
    reprocess: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    solvent_content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    estimated_asu_content_aa: Mapped[str] = mapped_column(
        String(255), nullable=False, default=""
    )
    export2run: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pointgroup_choices: Mapped[str] = mapped_column(Text, nullable=False, default="")


class Spreadsheet(Base):
    __tablename__ = "spreadsheets"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    esaf_id: Mapped[str] = mapped_column(String(20), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    data: Mapped[str] = mapped_column(Text, nullable=False) # JSON string of the spreadsheet data
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class Beamline(Base):
    __tablename__ = "beamlines"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)  # e.g., 23IDB
    alias: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)  # e.g., bl2


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)  # e.g., 2025-1
    start_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    end_date: Mapped[datetime] = mapped_column(Date, nullable=False)


class DayType(Base):
    __tablename__ = "day_types"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)  # e.g., User beam time
    color_code: Mapped[str] = mapped_column(String(7), nullable=False)  # Hex code e.g., #800080
    requires_staff: Mapped[bool] = mapped_column(Boolean, default=True)


class Staff(Base):
    __tablename__ = "staff"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(100), nullable=False)
    email: Mapped[str] = mapped_column(String(100), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class StaffQuota(Base):
    __tablename__ = "staff_quotas"
    __table_args__ = (
        UniqueConstraint("staff_id", "run_id", name="uq_staff_run_quota"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    staff_id: Mapped[int] = mapped_column(ForeignKey("staff.id"), nullable=False)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), nullable=False)
    max_days: Mapped[int] = mapped_column(Integer, default=0)
    max_weekends: Mapped[int] = mapped_column(Integer, default=0)


class StaffAvailability(Base):
    __tablename__ = "staff_availability"
    __table_args__ = (
        UniqueConstraint("staff_id", "date", name="uq_staff_date_avail"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    staff_id: Mapped[int] = mapped_column(ForeignKey("staff.id"), nullable=False)
    date: Mapped[datetime] = mapped_column(Date, nullable=False)
    preference: Mapped[str] = mapped_column(String(20), nullable=False)  # UNAVAILABLE, PREFERRED, NEUTRAL


class ScheduleDay(Base):
    __tablename__ = "schedule_days"
    __table_args__ = (
        UniqueConstraint("date", "beamline_id", name="uq_date_beamline"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(Date, nullable=False)
    beamline_id: Mapped[int] = mapped_column(ForeignKey("beamlines.id"), nullable=False)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), nullable=False)
    day_type_id: Mapped[int] = mapped_column(ForeignKey("day_types.id"), nullable=False)
    assigned_staff_id: Mapped[int] = mapped_column(ForeignKey("staff.id"), nullable=True)


class ShiftAllocation(Base):
    __tablename__ = "shift_allocations"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    schedule_day_id: Mapped[int] = mapped_column(ForeignKey("schedule_days.id"), nullable=False)
    shift_index: Mapped[int] = mapped_column(Integer, nullable=False)  # 1 or 2
    esaf_id: Mapped[str] = mapped_column(String(50), nullable=True)
    pi_name: Mapped[str] = mapped_column(String(100), nullable=True)
    project_id: Mapped[str] = mapped_column(String(50), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
