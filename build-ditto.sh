#! /bin/sh
# Build a portable Loglugger distribution archive (binaries + docs + examples + Ansible).
set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
cd "${ROOT}"

VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
DIST_DIR="${LOGLUGGER_DIST_DIR:-${ROOT}/dist}"
STAGE_NAME="loglugger-${VERSION}"
ARCHIVE_NAME="${STAGE_NAME}-linux-amd64.tar.gz"

echo "Building portable binaries (LOGLUGGER_PORTABLE=1)..."
LOGLUGGER_PORTABLE=1 ./build.sh

for bin in loglugger-client loglugger-server loglugger-extractor; do
	if [ ! -x "bin/${bin}" ]; then
		echo "error: missing binary after build: bin/${bin}" >&2
		exit 1
	fi
done

STAGE="$(mktemp -d)"
cleanup() {
	rm -rf "${STAGE}"
}
trap cleanup EXIT INT HUP TERM

DEST="${STAGE}/${STAGE_NAME}"
mkdir -pv "${DEST}/bin"

echo "Staging distribution under ${DEST}..."
cp -a bin/loglugger-client bin/loglugger-server bin/loglugger-extractor "${DEST}/bin/"
chmod 0755 "${DEST}/bin/loglugger-client" "${DEST}/bin/loglugger-server" "${DEST}/bin/loglugger-extractor"

# Operator-facing docs. SPECIFICATION.md (internal contract) and AGENTS.md
# (coding-agent guidance) stay in the repository and must not ship in dist.
cp -a README.md README-ru.md LICENSE CHANGELOG.md CHANGELOG-ru.md "${DEST}/"
cp -a examples "${DEST}/"

# Keep Ansible runnable from the unpacked tree: roles default to
# {{ playbook_dir }}/../../bin, which resolves to <archive-root>/bin.
if [ ! -f "${DEST}/examples/ansible/playbook.yml" ]; then
	echo "error: Ansible playbook missing from staged examples" >&2
	exit 1
fi
if [ ! -f "${DEST}/examples/ansible/inventory.example.ini" ]; then
	echo "error: Ansible inventory example missing from staged examples" >&2
	exit 1
fi
for required in CHANGELOG.md CHANGELOG-ru.md LICENSE README.md README-ru.md; do
	if [ ! -f "${DEST}/${required}" ]; then
		echo "error: required distribution file missing from stage: ${required}" >&2
		exit 1
	fi
done
for excluded in AGENTS.md SPECIFICATION.md; do
	if [ -e "${DEST}/${excluded}" ]; then
		echo "error: ${excluded} must not be included in the distribution archive" >&2
		exit 1
	fi
done

mkdir -pv "${DIST_DIR}"
ARCHIVE_PATH="${DIST_DIR}/${ARCHIVE_NAME}"
tar -C "${STAGE}" -czf "${ARCHIVE_PATH}" "${STAGE_NAME}"

echo "Distribution archive written to ${ARCHIVE_PATH}"
echo "Contents:"
tar -tzf "${ARCHIVE_PATH}" | sed -n '1,40p'
