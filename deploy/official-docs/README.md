# Official-document snapshot mount

This directory is mounted read-only at `/etc/fetchmark/official-docs` by both
Docker Compose deployments. `docindex` remains disabled until an operator:

1. places a validated v1 snapshot in this directory;
2. sets `FM_OFFICIAL_DOC_INDEX_FILE` to its container path, such as
   `/etc/fetchmark/official-docs/official-docs.json`; and
3. appends `docindex` to `FM_DISCOVERY_ENABLED_SOURCES`.

The distroless Fetchmark container runs as UID/GID `65532`. The mounted
directory and snapshot must be owned by `65532:65532`, must not be writable by
group or world, and must not contain symlink path components. On a Linux host,
an operator can prepare a snapshot with:

```bash
sudo chown 65532:65532 deploy/official-docs deploy/official-docs/official-docs.json
sudo chmod 0755 deploy/official-docs
sudo chmod 0644 deploy/official-docs/official-docs.json
```

Use `FM_OFFICIAL_DOC_INDEX_HOST_DIR` to bind a different host directory. The
snapshot is read once at startup; replacing it requires an atomic host-side
rename followed by a Fetchmark restart. Query handling never writes to or
refreshes this mount.
