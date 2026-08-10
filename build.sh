#! /bin/sh
set -eu

rm -rfv bin
mkdir -pv bin
VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
LDFLAGS="-X github.com/ydb-platform/loglugger/internal/buildinfo.Version=${VERSION}"

# Server and extractor do not need journald/CGO; keep them fully portable.
CGO_ENABLED=0 go build -ldflags "${LDFLAGS}" -o bin/loglugger-server ./cmd/server
CGO_ENABLED=0 go build -ldflags "${LDFLAGS}" -o bin/loglugger-extractor ./cmd/extractor

# Client needs CGO for sdjournal. Building on a newer distro (e.g. Ubuntu 22.04+)
# produces a binary that may fail on older hosts with:
#   version GLIBC_2.32 not found
# Use LOGLUGGER_PORTABLE=1 (requires zig) or ./build-docker.sh for older glibc targets.
build_client() {
	if [ "${LOGLUGGER_PORTABLE:-0}" = "1" ] || [ -n "${LOGLUGGER_GLIBC_VERSION:-}" ]; then
		glibc_version="${LOGLUGGER_GLIBC_VERSION:-2.31}"
		zig_bin="${ZIG:-}"
		if [ -z "${zig_bin}" ]; then
			if command -v zig >/dev/null 2>&1; then
				zig_bin="$(command -v zig)"
			fi
		fi
		if [ -z "${zig_bin}" ]; then
			echo "error: portable client build requires zig in PATH (or set ZIG=/path/to/zig)" >&2
			echo "hint: install zig, or use ./build-docker.sh instead" >&2
			exit 1
		fi
		if [ ! -d /usr/include/systemd ]; then
			echo "error: /usr/include/systemd not found; install libsystemd-dev (or equivalent)" >&2
			exit 1
		fi
		echo "Building loglugger-client with zig targeting glibc ${glibc_version}"
		wrapper="$(mktemp)"
		incdir="$(mktemp -d)"
		# Expose only systemd headers. Mixing the full host /usr/include with zig's
		# target libc breaks pthread initializers in runtime/cgo on newer distros.
		ln -s /usr/include/systemd "${incdir}/systemd"
		printf '%s\n' '#!/bin/sh' \
			"exec \"${zig_bin}\" cc -target x86_64-linux-gnu.${glibc_version} -I${incdir} \"\$@\"" \
			>"${wrapper}"
		chmod +x "${wrapper}"
		# netgo avoids zig/cgo unresolved libresolv symbols (res_search) while
		# still allowing CGO for sdjournal. -a avoids mixing cached cgo objects
		# built with a different CC/tag set.
		if ! CGO_ENABLED=1 CC="${wrapper}" go build -a -tags netgo -ldflags "${LDFLAGS}" -o bin/loglugger-client ./cmd/client; then
			rm -f "${wrapper}"
			rm -rf "${incdir}"
			exit 1
		fi
		rm -f "${wrapper}"
		rm -rf "${incdir}"
	else
		CGO_ENABLED=1 go build -ldflags "${LDFLAGS}" -o bin/loglugger-client ./cmd/client
	fi
}

build_client

if command -v objdump >/dev/null 2>&1; then
	max_glibc="$(
		objdump -T bin/loglugger-client 2>/dev/null \
			| sed -n 's/.*GLIBC_\([0-9.]*\).*/\1/p' \
			| sort -V \
			| tail -n 1 || true
	)"
	if [ -n "${max_glibc}" ]; then
		echo "loglugger-client requires glibc >= ${max_glibc} on the target host"
		# Warn when the binary needs newer glibc than Ubuntu 20.04 (2.31).
		newest="$(printf '%s\n' "${max_glibc}" "2.31" | sort -V | tail -n 1)"
		if [ "${newest}" = "${max_glibc}" ] && [ "${max_glibc}" != "2.31" ]; then
			if [ "${LOGLUGGER_PORTABLE:-0}" != "1" ] && [ -z "${LOGLUGGER_GLIBC_VERSION:-}" ]; then
				echo "warning: this may fail on older hosts (Ubuntu 20.04 has glibc 2.31)." >&2
				echo "warning: rebuild with LOGLUGGER_PORTABLE=1 ./build.sh or ./build-docker.sh" >&2
			fi
		fi
	fi
fi
