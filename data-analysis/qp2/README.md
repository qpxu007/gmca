# QP2

Crystallographic data processing and visualization platform.

## Install (editable)

```bash
pip install -e .
```

## CLI entry points

- `qp2-image-viewer` — Launch the diffraction image viewer.
- `qp2-data-viewer` — Launch the database-backed data viewer.
- `qp2-dp-server` — Start the data processing server.

## Notes

- Some components require facility services (Redis, MySQL, EPICS, SLURM). Configure via environment variables or config files where applicable.
