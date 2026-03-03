from PyQt5.QtCore import QThread, pyqtSignal, QObject
from epics import PV

from qp2.log.logging_config import get_logger

logger = get_logger(__name__)


# Generic Worker Signal Emitter
class WorkerSignals(QObject):
    """
    Defines the signals available from a running worker thread.
    Supported signals are:

    finished
        No data

    error
        tuple (exctype, value, traceback.format_exc() )

    result
        object data returned from processing, anything

    update_field
        tuple (field_name_or_identifier, new_value)
    """

    finished = pyqtSignal()
    error = pyqtSignal(tuple)
    result = pyqtSignal(object)
    update_field = pyqtSignal(str, object)  # Signal to update a specific field


class EpicsMonitorThread(QThread):
    signals = WorkerSignals()

    def __init__(self, pv_name_to_field_map):
        super().__init__()
        self.pv_name_to_field_map = (
            pv_name_to_field_map  # e.g., {"MOTOR:X.RBV": "lbeam_x_pv"}
        )
        self.pvs = {}
        self._is_running = True
        logger.info("EpicsMonitorThread initialized.")

    def pv_value_changed(self, pvname=None, value=None, **kwargs):
        field_identifier = self.pv_name_to_field_map.get(pvname)
        if field_identifier:
            logger.debug(f"PV {pvname} changed to {value}. Emitting update for {field_identifier}.")
            self.signals.update_field.emit(field_identifier, value)

    def run(self):
        logger.info("EpicsMonitorThread started.")
        try:
            for pv_name, field_id in self.pv_name_to_field_map.items():
                logger.info(f"Creating PV for {pv_name} mapped to {field_id}")
                pv = PV(pv_name)
                pv.add_callback(
                    lambda pvname=pv_name, value=None, **cb_kwargs: self.pv_value_changed(
                        pvname=pvname, value=cb_kwargs.get("value")
                    )
                )
                self.pvs[pv_name] = pv

            while self._is_running:
                self.msleep(100)  # Keep thread alive, callbacks are event-driven
        except Exception as e:
            logger.error("An error occurred in the EpicsMonitorThread.", exc_info=True)
            self.signals.error.emit((type(e), e, e.__traceback__))
        finally:
            self.signals.finished.emit()
            for pv_name, pv in self.pvs.items():
                logger.info(f"Clearing callbacks for {pv_name}")
                pv.clear_callbacks()
            logger.info("EpicsMonitorThread finished.")

    def stop(self):
        logger.info("Stopping EpicsMonitorThread.")
        self._is_running = False
