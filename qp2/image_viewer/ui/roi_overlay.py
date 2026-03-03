
import pyqtgraph as pg
from pyqtgraph.Qt import QtCore, QtGui, QtWidgets

class ROISelectionOverlay(pg.GraphicsObject):
    sigSelectionFinished = QtCore.pyqtSignal(object) # emits RectROI

    def __init__(self, view_box):
        super().__init__()
        self.view_box = view_box
        self.current_roi = None
        self.start_pos = None
        self.setZValue(10000) # Ensure it is on top of everything

    def boundingRect(self):
        # Return a very large rectangle to ensure we capture mouse events everywhere
        # Using reasonably large limits that shouldn't overflow
        return QtCore.QRectF(-1e10, -1e10, 2e10, 2e10)

    def paint(self, p, *args):
        pass # Transparent

    def mouseDragEvent(self, ev):
        if ev.button() != QtCore.Qt.MouseButton.LeftButton:
            ev.ignore()
            return

        # Enforce Ctrl key
        if not (ev.modifiers() & QtCore.Qt.ControlModifier):
            ev.ignore()
            return

        ev.accept()

        if ev.isStart():
            # Clear any existing ROI managed by this drag
            if self.current_roi:
                self.view_box.removeItem(self.current_roi)
                self.current_roi = None
            
            # Use mapSceneToView to get data coordinates
            self.start_pos = self.view_box.mapSceneToView(ev.buttonDownScenePos())
            
            self.current_roi = pg.RectROI(
                self.start_pos, 
                size=[0, 0], 
                pen=pg.mkPen('r', width=2), 
                movable=False, 
                resizable=False
            )
            self.view_box.addItem(self.current_roi)
        
        elif ev.isFinish():
            if self.current_roi:
                self.sigSelectionFinished.emit(self.current_roi)
                # We release the reference but don't remove it from the scene yet
                # The main window logic will handle the removal/cleanup
                self.current_roi = None 
        else:
            # Dragging update
            if self.current_roi:
                current_pos = self.view_box.mapSceneToView(ev.scenePos())
                
                # Calculate rect geometry (top-left x, y and width, height)
                x = min(self.start_pos.x(), current_pos.x())
                y = min(self.start_pos.y(), current_pos.y())
                w = abs(current_pos.x() - self.start_pos.x())
                h = abs(current_pos.y() - self.start_pos.y())
                
                self.current_roi.setPos([x, y])
                self.current_roi.setSize([w, h])
