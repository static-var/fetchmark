#!/bin/sh
set -eu

records=${FM_OPENPACK_SCALE_RECORDS:-100000}
memory=${FM_OPENPACK_SCALE_MEMORY:-512m}
cpus=${FM_OPENPACK_SCALE_CPUS:-2}

case "$records" in
  *[!0-9]*|'') echo "FM_OPENPACK_SCALE_RECORDS must be an integer" >&2; exit 2 ;;
esac
if [ "$records" -lt 10000 ] || [ "$records" -gt 100000 ]; then
  echo "FM_OPENPACK_SCALE_RECORDS must be 10000..100000" >&2
  exit 2
fi
memory_number=${memory%?}
memory_unit=${memory#"$memory_number"}
case "$memory_number" in
  *[!0-9]*|'') echo "FM_OPENPACK_SCALE_MEMORY must use a positive m/M/g/G suffix" >&2; exit 2 ;;
esac
if [ "$memory_number" -lt 1 ]; then
  echo "FM_OPENPACK_SCALE_MEMORY must use a positive m/M/g/G suffix" >&2
  exit 2
fi
case "$memory_unit" in
  m|M) memory_multiplier=1048576 ;;
  g|G) memory_multiplier=1073741824 ;;
  *) echo "FM_OPENPACK_SCALE_MEMORY must use a positive m/M/g/G suffix" >&2; exit 2 ;;
esac
if [ "$memory_number" -gt 1048576 ]; then
  echo "FM_OPENPACK_SCALE_MEMORY is too large" >&2
  exit 2
fi
expected_memory_bytes=$((memory_number * memory_multiplier))
case "$cpus" in
  *[!0-9]*|'') echo "FM_OPENPACK_SCALE_CPUS must be a positive integer" >&2; exit 2 ;;
esac
if [ "$cpus" -lt 1 ]; then
  echo "FM_OPENPACK_SCALE_CPUS must be a positive integer" >&2
  exit 2
fi

architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) goarch=arm64 ;;
  x86_64|amd64) goarch=amd64 ;;
  *) echo "unsupported Docker architecture: $architecture" >&2; exit 2 ;;
esac

work=$(mktemp -d "${TMPDIR:-/tmp}/fetchmark-openpack-scale.XXXXXX")
cleanup() {
  case "$work" in
    */fetchmark-openpack-scale.*) rm -rf -- "$work" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

GOOS=linux GOARCH="$goarch" CGO_ENABLED=0 \
  go test -c -o "$work/openpack-scale.test" ./internal/adapters/openpackindex
cp deploy/openpack-scale.Dockerfile "$work/Dockerfile"

image="fetchmark-openpack-scale:local-$goarch"
docker build --network=none --tag "$image" "$work"
docker run --rm \
  --network=none \
  --memory="$memory" \
  --memory-swap="$memory" \
  --cpus="$cpus" \
  --pids-limit=128 \
  --read-only \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --mount=type=volume,destination=/work,volume-nocopy \
  --env TMPDIR=/work \
  --env FM_OPENPACK_SCALE_RECORDS="$records" \
  --env FM_OPENPACK_SCALE_EXPECT_CGROUP_MEMORY_BYTES="$expected_memory_bytes" \
  "$image" \
  -test.run '^TestOpenPackScaleProfile$' -test.count=1 -test.v -test.timeout=30m
