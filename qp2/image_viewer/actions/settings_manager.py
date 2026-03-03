from PyQt5.QtCore import QObject, pyqtSignal

from qp2.image_viewer.config import DEFAULT_SETTINGS


class SettingsManager(QObject):
    # Now emits (new_settings, changed_keys) where changed_keys is a dict of key: (old, new)
    settings_changed = pyqtSignal(dict, dict)

    def __init__(self):
        super().__init__()
        self._settings = DEFAULT_SETTINGS.copy()

    def get(self, key, default=None):
        return self._settings.get(key, default)

    def set(self, key, value):
        old_value = self._settings.get(key)
        if old_value != value:
            self._settings[key] = value
            self.settings_changed.emit(self._settings.copy(), {key: (old_value, value)})

    def as_dict(self):
        return self._settings.copy()

    def update_from_dict(self, new_settings):
        changed = {}
        for k, v in new_settings.items():
            old_v = self._settings.get(k)
            if old_v != v:
                changed[k] = (old_v, v)
                self._settings[k] = v
        if changed:
            self.settings_changed.emit(self._settings.copy(), changed)

    def restore_defaults(self):
        changed = {}
        for k, v in DEFAULT_SETTINGS.items():
            old_v = self._settings.get(k)
            if old_v != v:
                changed[k] = (old_v, v)
                self._settings[k] = v
        if changed:
            self.settings_changed.emit(self._settings.copy(), changed)
