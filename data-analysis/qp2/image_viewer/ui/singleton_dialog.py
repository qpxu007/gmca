from PyQt5.QtWidgets import QDialog


class SingletonDialog(QDialog):
    _instance = None
    _is_showing = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            try:
                if cls._is_showing:
                    return cls._instance
                else:
                    return cls._instance
            except RuntimeError:
                cls._instance = None
                cls._is_showing = False
        instance = super().__new__(cls)
        cls._instance = instance
        cls._is_showing = False
        return instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if getattr(self, '_initialized', False):
            return
        self._initialized = True
        self.finished.connect(self._on_finished)

    def show(self):
        if self.__class__._is_showing:
            self.raise_()
            self.activateWindow()
        else:
            self.__class__._is_showing = True
            super().show()

    def _on_finished(self, result):
        type(self)._instance = None
        type(self)._is_showing = False
        self._initialized = False

    def _apply_common_fallback(self, widget, plugin_value, common_value):
        """Pre-fill widget with common value if plugin has no override. Show red if overridden."""
        if plugin_value:
            widget.setText(plugin_value)
        elif common_value:
            widget.setText(common_value)
        self._style_override(widget, common_value)
        widget.textChanged.connect(lambda _: self._style_override(widget, common_value))

    def _style_override(self, widget, common_value):
        """Red text when widget value differs from common setting."""
        text = widget.text().strip()
        if text and common_value and text != common_value:
            widget.setStyleSheet("color: red;")
        else:
            widget.setStyleSheet("")

    def _apply_common_spinbox_fallback(self, spinbox, plugin_value, common_value):
        """Pre-fill spinbox with common value if plugin has no override. Show red if overridden."""
        effective_common = common_value or 0
        at_auto = not plugin_value or plugin_value <= spinbox.minimum()
        if at_auto and effective_common:
            spinbox.setValue(effective_common)
        self._style_spinbox_override(spinbox, effective_common)
        spinbox.valueChanged.connect(lambda _: self._style_spinbox_override(spinbox, effective_common))

    def _style_spinbox_override(self, spinbox, common_value):
        """Red text when spinbox value differs from common setting."""
        val = spinbox.value()
        if val > spinbox.minimum() and common_value and val != common_value:
            spinbox.setStyleSheet("color: red;")
        else:
            spinbox.setStyleSheet("")
