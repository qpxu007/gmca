import pyqtgraph.console as pgc
from pyqtgraph.Qt import QtWidgets, QtCore
import numpy as np
import rlcompleter

class PythonConsoleWidget(pgc.ConsoleWidget):
    def __init__(self, namespace=None, text=None):
        if namespace is None:
            namespace = {}
        
        # Add common libraries to namespace
        namespace['np'] = np
        
        super().__init__(namespace=namespace, text=text)
        self.setWindowTitle("Python Console")
        
        # Setup Completer
        self.completer = rlcompleter.Completer(self.localNamespace)
        
        # Install event filter on the input widget to capture Tab key
        self.input.installEventFilter(self)

    def push_vars(self, new_vars):
        """
        Update the console namespace with new variables.
        """
        self.localNamespace.update(new_vars)

    def eventFilter(self, obj, event):
        if obj == self.input and event.type() == QtCore.QEvent.KeyPress:
            if event.key() == QtCore.Qt.Key_Tab:
                self._handle_tab_completion()
                return True  # Consume the event (prevent focus change)
        return super().eventFilter(obj, event)

    def _handle_tab_completion(self):
        # 1. Get current text and cursor position
        # CmdInput inherits from QLineEdit
        text = self.input.text()
        pos = self.input.cursorPosition()
        
        # Get text up to cursor
        text_upto_cursor = text[:pos]
        
        # 2. Find the word being typed. 
        # Simple regex or split: find the last token that looks like a python name/attr
        import re
        match = re.search(r'([\w\.]+)$', text_upto_cursor)
        if not match:
            return
        
        token = match.group(1)
        
        # 3. Use rlcompleter to find matches
        # rlcompleter needs the full namespace (locals + globals + builtins)
        
        # Merge namespaces for completion (prioritize local)
        ns = {}
        if hasattr(self, 'globals'):
            # self.globals is likely a method in ConsoleWidget base class
            g = self.globals()
            if isinstance(g, dict):
                ns.update(g)
        ns.update(self.localNamespace)
        
        # We create a new completer instance or reuse
        # Recreating ensures we have the latest namespace
        completer = rlcompleter.Completer(ns)
        
        matches = []
        state = 0
        while True:
            match_str = completer.complete(token, state)
            if match_str is None:
                break
            matches.append(match_str)
            state += 1
            
        if not matches:
            return
            
        # 4. Handle matches
        if len(matches) == 1:
            # Single match: complete it
            completion = matches[0]
            # Calculate what to append
            remainder = completion[len(token):]
            self.input.insert(remainder)
            
        else:
            # Multiple matches: print them to the output
            # Common prefix?
            def common_prefix(m):
                if not m: return ''
                s1 = min(m)
                s2 = max(m)
                for i, c in enumerate(s1):
                    if c != s2[i]:
                        return s1[:i]
                return s1

            prefix = common_prefix(matches)
            if len(prefix) > len(token):
                remainder = prefix[len(token):]
                self.input.insert(remainder)
            
            # Print options
            self.output.insertPlainText('\n' + '  '.join(matches) + '\n')
            self.input.setFocus() # Ensure focus remains on input
