from pathlib import Path
from typing import Dict, List


def get_image_set_string(run_map: Dict[str, List[int]]) -> str:
    """
    Creates a standardized, descriptive imageSet string from a run mapping.
    Format examples:
      - single series: "prefix:1-1800" or "prefix:1,91"
      - multi series:  "prefix1:1-prefix2:1"
      - very complex: "5 series (prefix...)"
    """
    if not run_map:
        return "N/A"

    parts = []
    # Sort keys to ensure deterministic output
    for master_path in sorted(run_map.keys()):
        frames = run_map[master_path]
        if not frames:
            # Assume all frames if list is empty but key exists
            prefix = Path(master_path).stem.replace("_master", "")
            parts.append(f"{prefix}:all")
            continue

        prefix = Path(master_path).stem.replace("_master", "")

        # Sort frames to detect ranges and avoid duplicates
        sorted_frames = sorted(list(set(frames)))

        # Heuristic: Check if it's a contiguous range efficiently
        is_contiguous = (
            len(sorted_frames) > 1 
            and (sorted_frames[-1] - sorted_frames[0] + 1) == len(sorted_frames)
        )

        if len(sorted_frames) == 1:
            frame_part = str(sorted_frames[0])
        elif is_contiguous:
            frame_part = f"{min(sorted_frames)}-{max(sorted_frames)}"
        else:
            # Join individual frames, but cap if too many
            if len(sorted_frames) > 10:
                frame_part = (
                    f"{sorted_frames[0]}...{sorted_frames[-1]} ({len(sorted_frames)} frames)"
                )
            else:
                frame_part = ",".join(map(str, sorted_frames))

        parts.append(f"{prefix}:{frame_part}")

    if not parts:
        return "None"

    # For a large number of series, return a summary to keep DB strings manageable
    if len(parts) > 3:
        return f"{len(parts)} series ({parts[0].split(':')[0]}...)"

    # Use '-' as joiner if it's a multi-master strategy run (legacy convention) 
    # but use ', ' for general readability if they are distinct series.
    # To keep it standardized, let's use '|' or ', '
    return ", ".join(parts)
