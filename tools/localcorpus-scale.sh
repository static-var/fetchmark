#!/bin/sh
set -eu

records=${FM_LOCALCORPUS_SCALE_RECORDS:-10000}
cpus=${FM_LOCALCORPUS_SCALE_CPUS:-2}

case "$records" in
  10000|50000|100000) ;;
  *) echo "FM_LOCALCORPUS_SCALE_RECORDS must be 10000, 50000, or 100000" >&2; exit 2 ;;
esac

if [ -n "${FM_LOCALCORPUS_SCALE_MEMORY:-}" ]; then
  memory=$FM_LOCALCORPUS_SCALE_MEMORY
else
  case "$records" in
    10000) memory=512m ;;
    50000) memory=768m ;;
    100000) memory=1024m ;;
  esac
fi

memory_number=${memory%?}
memory_unit=${memory#"$memory_number"}
case "$memory_number" in
  *[!0-9]*|'') echo "FM_LOCALCORPUS_SCALE_MEMORY must use a positive m/M/g/G suffix" >&2; exit 2 ;;
esac
if [ "$memory_number" -lt 1 ]; then
  echo "FM_LOCALCORPUS_SCALE_MEMORY must use a positive m/M/g/G suffix" >&2
  exit 2
fi
case "$memory_unit" in
  m|M) memory_multiplier=1048576 ;;
  g|G) memory_multiplier=1073741824 ;;
  *) echo "FM_LOCALCORPUS_SCALE_MEMORY must use a positive m/M/g/G suffix" >&2; exit 2 ;;
esac
if [ "$memory_number" -gt 1048576 ]; then
  echo "FM_LOCALCORPUS_SCALE_MEMORY is too large" >&2
  exit 2
fi
expected_memory_bytes=$((memory_number * memory_multiplier))

case "$cpus" in
  *[!0-9]*|'') echo "FM_LOCALCORPUS_SCALE_CPUS must be a positive integer" >&2; exit 2 ;;
esac
if [ "$cpus" -lt 1 ]; then
  echo "FM_LOCALCORPUS_SCALE_CPUS must be a positive integer" >&2
  exit 2
fi

architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) goarch=arm64 ;;
  x86_64|amd64) goarch=amd64 ;;
  *) echo "unsupported Docker architecture: $architecture" >&2; exit 2 ;;
esac

work=$(mktemp -d "${TMPDIR:-/tmp}/fetchmark-localcorpus-scale.XXXXXX")
cleanup() {
  case "$work" in
    */fetchmark-localcorpus-scale.*) rm -rf -- "$work" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

GOOS=linux GOARCH="$goarch" CGO_ENABLED=0 \
  go test -c -o "$work/localcorpus-scale.test" ./internal/adapters/bleveindex
cp deploy/localcorpus-scale.Dockerfile "$work/Dockerfile"

image="fetchmark-localcorpus-scale:local-$goarch"
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
  --env GOMAXPROCS="$cpus" \
  --env FM_LOCALCORPUS_SCALE_RECORDS="$records" \
  --env FM_LOCALCORPUS_SCALE_EXPECT_CGROUP_MEMORY_BYTES="$expected_memory_bytes" \
  "$image" \
  -test.run '^TestLocalCorpusScaleProfile$' -test.count=1 -test.v -test.timeout=55m
