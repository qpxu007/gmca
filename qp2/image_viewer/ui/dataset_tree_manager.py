from PyQt5 import QtWidgets, QtCore


class DatasetTreeManager(QtCore.QObject):
    """
    Manages the dataset history tree widget, including updates and filtering.
    """

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.ui_manager = main_window.ui_manager
        self.dataset_manager = main_window.dataset_manager
        
        # Filter state
        self.is_filtered = False
        self.filter_mode = None  # "show_containing_text" or "hide_containing_text"
        self.filter_text = ""

    def update_tree(self):
        """Updates the tree widget with current dataset data and reapplies filter if active."""
        sample_data = self.dataset_manager.get_all_data()
        self.ui_manager.update_dataset_tree(sample_data)
        
        # Reapply filter if needed
        if self.is_filtered and self.filter_mode and self.filter_text:
            self.apply_filter(
                filter_mode=self.filter_mode,
                filter_text=self.filter_text,
            )

    def apply_filter(self, filter_mode="show_containing_text", filter_text=None):
        """Applies a text filter to the dataset tree."""
        if filter_text is None:
            prompt_label = (
                "Show items containing:"
                if filter_mode == "show_containing_text"
                else "Hide items containing:"
            )
            text, ok = QtWidgets.QInputDialog.getText(
                self.main_window, "Filter Datasets", prompt_label
            )
            if not ok or not text:
                return
            filter_text = text.lower()
        else:
            filter_text = filter_text.lower()
            
        # Store filter state
        self.filter_mode = filter_mode
        self.filter_text = filter_text
        self.is_filtered = True

        # Apply filter to the tree (3-level structure: Sample -> Run -> Dataset)
        root = self.ui_manager.dataset_tree_widget.invisibleRootItem()
        
        for i in range(root.childCount()):
            sample_item = root.child(i)
            any_sample_child_visible = False
            
            for j in range(sample_item.childCount()):
                run_item = sample_item.child(j)
                any_run_child_visible = False
                
                for k in range(run_item.childCount()):
                    dataset_item = run_item.child(k)
                    # Filter is based on the full path tooltip
                    item_text = dataset_item.toolTip(0).lower()
                    is_match = filter_text in item_text

                    is_hidden = (
                        not is_match if filter_mode == "show_containing_text" else is_match
                    )
                    dataset_item.setHidden(is_hidden)
                    if not is_hidden:
                        any_run_child_visible = True
                        any_sample_child_visible = True

                # Hide the run if all its children are hidden
                run_item.setHidden(not any_run_child_visible)
            
            # Hide the sample if all its children are hidden
            sample_item.setHidden(not any_sample_child_visible)

    def clear_filter(self):
        """Removes any active filter and shows all tree items."""
        root = self.ui_manager.dataset_tree_widget.invisibleRootItem()
        for i in range(root.childCount()):
            sample_item = root.child(i)
            sample_item.setHidden(False)
            for j in range(sample_item.childCount()):
                run_item = sample_item.child(j)
                run_item.setHidden(False)
                for k in range(run_item.childCount()):
                    dataset_item = run_item.child(k)
                    dataset_item.setHidden(False)

        self.is_filtered = False
        self.filter_mode = None
        self.filter_text = ""
        self.ui_manager.show_status_message("Dataset history filter cleared.", 3000)
