"""
The script contains ...
"""
import shutil
import functools
import uuid
from pyspark.sql import DataFrame


class TempCheckpointDisplay:
    """
    Wrap Databricks display() so streaming DataFrames get a managed checkpoint.

    tckp_display = TempCheckpointDisplay(display, "/Volumes/path/to/adhoc_checkpoints")

    tckp_display(stream)    # Calls on display() using a temporary checkpoint
    tckp_display.cleanup()  # Deletes all temporary checkpoints it has created during the session

    Batch DataFrames bypass the checkpoint creation and just uses the native display()
    """

    def __init__(self, display_fn, base_path):
        self._display = display_fn
        self.base_path = base_path
        self._checkpoints = []
    
    def __call__(self, df=None, *args, name=None, base=None, **kwargs):
        # In case a non-streaming table is passed into the function,
        # just call display() normally
        if not getattr(df, "isStreaming", False) or "checkpointLocation" in kwargs:
            return self._display(df, *args, **kwargs)

        path = self._new_path(name, base)
        kwargs["checkpointLocation"] = path
        print(f"[tckp_display] created checkpoint: {path}")

        if path not in self._checkpoints:
            self._checkpoints.append(path)
            return self._display(df, *args, **kwargs)

    # Delete all persisted checkpoints
    def cleanup(self):
        for path in list(self._checkpoints):
            self._remove(path)
            self._checkpoints.remove(path)

    # Create the checkpoint path with either the predefined
    # directory name and base path or generate a random directory 
    def _new_path(self, name, base):
        root = (base or self.base_path).rstrip("/")
        return f"{root}/{name or uuid.uuid4().hex[:12]}"

    @staticmethod
    def _remove(path):
        try:
            shutil.rmtree(path, ignore_errors=True)
            print(f"[tckp_display] removed {path}")
        except Exception as e:
            print(f"[tckp_display] could not remove {path}: {e}")


def register_tckp_display(display, base_path):
    """Create a TempCheckpointDisplay and attach it to DataFrame,
    so both tckp_display(df) and df.tckp_display() work."""
    tckp = TempCheckpointDisplay(display, base_path)

    def _method(self, **kwargs):
        return tckp(self, **kwargs)

    DataFrame.tckp_display = _method
    return tckp

