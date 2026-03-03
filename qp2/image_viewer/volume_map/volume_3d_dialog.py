# In qp2/image_viewer/ui/volume_3d_dialog.py

import numpy as np
import pyqtgraph.opengl as gl
from pyqtgraph.Qt import QtWidgets, QtGui  # Ensure QtGui is imported

from qp2.log.logging_config import get_logger

# gl = None  # module cache
# def _ensure_gl():
#     global gl
#     if gl is None:
#         import importlib, importlib.util
#         if importlib.util.find_spec("pyqtgraph.opengl") is None:
#             raise RuntimeError("pyqtgraph.opengl not available")
#         gl = importlib.import_module("pyqtgraph.opengl")
#     return gl

logger = get_logger(__name__)


class Volume3dDialog(QtWidgets.QDialog):
    """A dialog for displaying 3D hotspot ellipsoids or cuboids in an OpenGL scene."""

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("3D Hotspot Visualization")
        self.setMinimumSize(800, 700)

        layout = QtWidgets.QVBoxLayout(self)

        control_layout = QtWidgets.QHBoxLayout()
        control_layout.addWidget(QtWidgets.QLabel("Display Mode:"))
        self.mode_selector = QtWidgets.QComboBox()
        self.mode_selector.addItems(["Cuboid (Wireframe)", "Ellipsoid (Shaded)"])
        self.mode_selector.currentIndexChanged.connect(self._redraw_hotspots)
        control_layout.addWidget(self.mode_selector)

        control_layout.addSpacing(20)
        control_layout.addWidget(QtWidgets.QLabel("Voxel Size:"))
        self.voxel_size_spinner = QtWidgets.QDoubleSpinBox()
        self.voxel_size_spinner.setSuffix(" µm")
        self.voxel_size_spinner.setDecimals(2)
        self.voxel_size_spinner.setSingleStep(0.1)
        self.voxel_size_spinner.setRange(0.0, 1000.0)
        self.voxel_size_spinner.setValue(0.0)
        self.voxel_size_spinner.setToolTip(
            "Set to > 0 to display hotspot dimensions in microns"
        )
        self.voxel_size_spinner.valueChanged.connect(self._redraw_hotspots)
        control_layout.addWidget(self.voxel_size_spinner)

        control_layout.addSpacing(20)
        self.bg_color_btn = QtWidgets.QPushButton("Background Color")
        self.bg_color_btn.clicked.connect(self._on_change_background)
        control_layout.addWidget(self.bg_color_btn)

        control_layout.addStretch(1)
        layout.addLayout(control_layout)

        self.view_3d = gl.GLViewWidget()
        layout.addWidget(self.view_3d)

        # --- FIX 1: Hide the grid by commenting it out ---
        # grid = gl.GLGridItem()
        # self.view_3d.addItem(grid)

        self._add_labeled_axes()
        self.bounding_box_mesh = None

        self.hotspot_meshes = []
        self.hotspots_data = []

    def _add_labeled_axes(self, size=50):
        """Creates three labeled lines for X, Y, and Z axes."""
        x_axis = gl.GLLinePlotItem(
            pos=np.array([[0, 0, 0], [size, 0, 0]]), color=(1, 0, 0, 1), width=5
        )
        x_label = gl.GLTextItem(
            pos=(size * 1.05, 0, 0), text="X - gonio", font=QtGui.QFont("Helvetica", 14)
        )

        y_axis = gl.GLLinePlotItem(
            pos=np.array([[0, 0, 0], [0, size, 0]]), color=(0, 1, 0, 1), width=5
        )
        y_label = gl.GLTextItem(
            pos=(0, size * 1.05, 0),
            text="Y - gravity",
            font=QtGui.QFont("Helvetica", 14),
        )

        z_axis = gl.GLLinePlotItem(
            pos=np.array([[0, 0, 0], [0, 0, size]]), color=(0, 0, 1, 1), width=5
        )
        z_label = gl.GLTextItem(
            pos=(0, 0, size * 1.05), text="Z - X-ray", font=QtGui.QFont("Helvetica", 14)
        )

        self.view_3d.addItem(x_axis)
        self.view_3d.addItem(x_label)
        self.view_3d.addItem(y_axis)
        self.view_3d.addItem(y_label)
        self.view_3d.addItem(z_axis)
        self.view_3d.addItem(z_label)

    def _draw_bounding_box(self, volume_shape: tuple):
        """Draws a wireframe box around the entire volume."""
        if self.bounding_box_mesh:
            self.view_3d.removeItem(self.bounding_box_mesh)

        z_max, y_max, x_max = volume_shape

        verts = np.array(
            [
                [0, 0, 0],
                [x_max, 0, 0],
                [x_max, y_max, 0],
                [0, y_max, 0],
                [0, 0, z_max],
                [x_max, 0, z_max],
                [x_max, y_max, z_max],
                [0, y_max, z_max],
            ]
        )

        edges = np.array(
            [
                [0, 1],
                [1, 2],
                [2, 3],
                [3, 0],  # bottom
                [4, 5],
                [5, 6],
                [6, 7],
                [7, 4],  # top
                [0, 4],
                [1, 5],
                [2, 6],
                [3, 7],  # sides
            ]
        )

        self.bounding_box_mesh = gl.GLLinePlotItem(
            pos=verts[edges.flatten()],
            color=(0.5, 0.5, 0.5, 0.5),
            width=3,
            mode="lines",
        )
        self.view_3d.addItem(self.bounding_box_mesh)

    def clear_hotspots(self):
        """Removes all previously drawn hotspot meshes from the scene."""
        for item in self.hotspot_meshes:
            self.view_3d.removeItem(item)
        self.hotspot_meshes = []

    def add_hotspots(self, hotspots: list, volume_shape: tuple):
        """
        Stores the new hotspot data and triggers the initial drawing.
        """
        self.hotspots_data = hotspots
        if not hotspots:
            self.clear_hotspots()
            return

        self._draw_bounding_box(volume_shape)

        z_max, y_max, x_max = volume_shape

        # set orientation and distance of the camera
        self.view_3d.setCameraPosition(
            distance=max(volume_shape) * 2.5, elevation=90, azimuth=-90
        )

        center_vector = QtGui.QVector3D(x_max / 2, y_max / 2, z_max / 2)
        self.view_3d.opts["center"] = center_vector

        self._redraw_hotspots()

    def _redraw_hotspots(self):
        """Clears and redraws all hotspots based on the current display mode."""
        self.clear_hotspots()

        mode = self.mode_selector.currentText()

        for i, hotspot in enumerate(self.hotspots_data):
            if "Cuboid" in mode:
                self._draw_cuboid(hotspot, i)
            else:
                self._draw_ellipsoid(hotspot, i)

            self._add_size_label(hotspot)
            self._add_center_marker(hotspot)

    def _create_transform(
            self, hotspot: dict, scale_factor: float = 1.0
    ) -> QtGui.QMatrix4x4:
        """
        Builds a single, complete transformation matrix for a hotspot using the
        standard Translate * Rotate * Scale order.
        """
        center = hotspot["coords"]
        dimensions = np.array(hotspot["dimensions"]) * scale_factor
        orientation = hotspot["orientation"]

        transform = QtGui.QMatrix4x4()
        transform.translate(center[0], center[1], center[2])

        rot = orientation.T
        rotation_matrix = QtGui.QMatrix4x4(
            rot[0, 0],
            rot[0, 1],
            rot[0, 2],
            0,
            rot[1, 0],
            rot[1, 1],
            rot[1, 2],
            0,
            rot[2, 0],
            rot[2, 1],
            rot[2, 2],
            0,
            0,
            0,
            0,
            1,
        )
        transform.rotate(
            QtGui.QQuaternion.fromRotationMatrix(rotation_matrix.normalMatrix())
        )
        transform.scale(dimensions[0], dimensions[1], dimensions[2])

        return transform

    def _add_size_label(self, hotspot: dict):
        """Adds a text label showing the dimensions of the hotspot."""
        center = hotspot["coords"]
        dims = hotspot["dimensions"]

        voxel_size = self.voxel_size_spinner.value()

        if voxel_size > 0.0:
            dims_in_microns = np.array(dims) * voxel_size
            unit = "µm"
            label_text = f"Size: {dims_in_microns[0]:.1f}x{dims_in_microns[1]:.1f}x{dims_in_microns[2]:.1f} {unit}"
        else:
            unit = "vx"
            label_text = f"Size: {dims[0]:.1f}x{dims[1]:.1f}x{dims[2]:.1f} {unit}"

        label_pos = (center[0], center[1], center[2] + max(dims) * 0.6)
        text_item = gl.GLTextItem(
            pos=label_pos, text=label_text, color="w", font=QtGui.QFont("Helvetica", 14)
        )

        self.view_3d.addItem(text_item)
        self.hotspot_meshes.append(text_item)

    def _add_center_marker(self, hotspot: dict):
        """Draws a marker and a coordinate label at the center of the hotspot."""
        center = hotspot["coords"]
        # Draw the yellow point marker
        marker = gl.GLScatterPlotItem(
            pos=np.array([center]), color=(1, 1, 0, 1), size=10
        )
        self.view_3d.addItem(marker)
        self.hotspot_meshes.append(marker)

        coord_text = f"Center: ({center[0]},{center[1]},{center[2]})"
        label_pos = (center[0], center[1], center[2])
        logger.debug(f"Adding coordinate label at {label_pos} with text: {coord_text}")
        coord_label = gl.GLTextItem(
            pos=label_pos,
            text=coord_text,
            color="y",
            font=QtGui.QFont("Helvetica", 14),
        )
        self.view_3d.addItem(coord_label)
        self.hotspot_meshes.append(coord_label)

    def _draw_cuboid(self, hotspot: dict, index: int):
        """Draws a single hotspot as a wireframe cuboid."""
        box_item = gl.GLBoxItem()
        box_item.setGLOptions("opaque")

        intensity_factor = 1.0 - (index / (len(self.hotspots_data) + 1))
        face_color = (0.8, intensity_factor, 0.2, 0.2)
        box_item.setColor(face_color)

        transform = self._create_transform(hotspot, scale_factor=1.0)

        correction = QtGui.QMatrix4x4()
        correction.translate(-0.5, -0.5, -0.5)
        final_transform = transform * correction
        box_item.setTransform(final_transform)

        self.view_3d.addItem(box_item)
        self.hotspot_meshes.append(box_item)

    def _draw_ellipsoid(self, hotspot: dict, index: int):
        """Draws a single hotspot as a shaded ellipsoid."""
        mesh_data = gl.MeshData.sphere(rows=10, cols=20)
        mesh_item = gl.GLMeshItem(
            meshdata=mesh_data, smooth=True, shader="shaded", glOptions="opaque"
        )

        intensity_factor = 1.0 - (index / (len(self.hotspots_data) + 1))
        color = (1.0, intensity_factor, 0.2, 0.7)
        mesh_item.setColor(color)

        transform = self._create_transform(hotspot, scale_factor=0.5)
        mesh_item.setTransform(transform)

        self.view_3d.addItem(mesh_item)
        self.hotspot_meshes.append(mesh_item)

    def _on_change_background(self):
        """Opens a color dialog to change the 3D view background."""
        color = QtWidgets.QColorDialog.getColor()
        if color.isValid():
            self.view_3d.setBackgroundColor(color)
