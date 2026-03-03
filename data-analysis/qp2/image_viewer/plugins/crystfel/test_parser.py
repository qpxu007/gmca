import sys
from qp2.image_viewer.plugins.crystfel.stream_utils import StreamParser

if __name__ == "__main__":
    a = StreamParser(
        "/home/qxu/data-analysis/qp2/image_viewer/plugins/crystfel/crystfel.stream.example",
        high_res_limit=3,
        max_reflections=5,
    )

    a = StreamParser(
        "/home/qxu/crystfel/B1_ras_run1_R7_master/crystfel.stream",
        high_res_limit=3,
        max_reflections=5,
    )

    print(a.all_results)
    sys.exit(0)
    for r in a.all_results:
        print(r["image_filename"], "num_peaks=", r["num_peaks"], "cell=", r["unit_cell_crystfel"])
        if "reflections_crystfel" in r:
            print("Reflections:", len(r["reflections_crystfel"]))
            for refl in r["reflections_crystfel"][:5]:
                print("  ", refl)
        if "spots_crystfel" in r:
            print("Spots:", len(r["spots_crystfel"]))
            for spot in r["spots_crystfel"][:5]:
                print("  ", spot)
        print("-" * 40)
        if "indexed_by" in r:
            print("Indexed by:", r["indexed_by"])
        if "lattice_type" in r:
            print("Lattice type:", r["lattice_type"])
        if "centering" in r:
            print("Centering:", r["centering"])
        if "event_num" in r:
            print("Event number:", r["event_num"])
        if "image_serial_number" in r:
            print("Image serial number:", r["image_serial_number"])
        if "img_num" in r:  # This is the absolute frame index
            print("Absolute frame index:", r["img_num"])

        if "chunk" in r:
            print("Chunk info:", r["chunk"][:10])