def merge_dicts(a: dict, b: dict) -> dict:
    # returns a new dict; does not mutate a or b
    out = {}
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        av = a.get(k)
        bv = b.get(k)
        if isinstance(av, dict) and isinstance(bv, dict):
            out[k] = merge_dicts(av, bv)  # both dicts: merge recursively [1][3][10]
        elif bv is not None:
            out[k] = bv  # prefer b's non-None value [1][3]
        else:
            out[k] = av  # b is None or missing: keep a's value [1][3]
    return out
