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
