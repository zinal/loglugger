#! /bin/sh
# Build Loglugger binaries inside Ubuntu 20.04 (glibc 2.31) so the CGO-linked
# client runs on older hosts that reject binaries built on newer distros.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
cd "${ROOT}"

if command -v docker >/dev/null 2>&1; then
	ENGINE=docker
elif command -v podman >/dev/null 2>&1; then
	ENGINE=podman
else
	echo "error: docker or podman is required for ./build-docker.sh" >&2
	echo "hint: alternatively install zig and run: LOGLUGGER_PORTABLE=1 ./build.sh" >&2
	exit 1
fi

IMAGE_TAG="${LOGLUGGER_BUILD_IMAGE:-loglugger-build:glibc231}"
GO_VERSION="${LOGLUGGER_GO_VERSION:-1.26.5}"

"${ENGINE}" build \
	--build-arg "GO_VERSION=${GO_VERSION}" \
	-f Dockerfile.build \
	-t "${IMAGE_TAG}" \
	.

mkdir -pv bin
"${ENGINE}" run --rm \
	-v "${ROOT}:/src:z" \
	-w /src \
	-e HOME=/tmp \
	-u "$(id -u):$(id -g)" \
	"${IMAGE_TAG}" \
	./build.sh

echo "Portable binaries written to ${ROOT}/bin"
