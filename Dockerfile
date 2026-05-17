# =============================================================================
# Delta · production container
#
# Multi-stage build:
#   1) builder  — Debian slim + clang/cmake, compiles delta_server in Release.
#   2) runtime  — distroless-style minimal Debian, only the static-linked
#                 binary + LICENSE end up here.
#
# Usage:
#   docker build -t delta:latest .
#   docker run -d --name delta \
#       -p 16888:16888 -p 16889:16889 -p 16890:16890 \
#       -v $HOME/.deltaql:/data \
#       delta:latest
#
# Ports: 16888 HTTP REST, 16889 deltaql binary, 16890 WebSocket.
# Data:  /data (mount a volume — corresponds to ~/.deltaql inside the image).
# =============================================================================

# ---------- 1) builder --------------------------------------------------------
FROM debian:bookworm-slim AS builder

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential cmake ninja-build clang git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt ./
COPY third_party/ ./third_party/
COPY src/        ./src/
COPY tests/      ./tests/
COPY LICENSE     ./

# Reproducible Release build with all warnings as just warnings (the codebase
# is -Wall clean but we don't want a stray new warning to fail the image).
ENV CXX=clang++
ENV CXXFLAGS="-O3 -DNDEBUG"
RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_COMPILER=clang++ \
 && cmake --build build --parallel \
 && ctest --test-dir build --output-on-failure

# ---------- 2) runtime --------------------------------------------------------
# P0-17: distroless cc image. No shell, no apt, no netcat, no tini. Just
# libstdc++ + libgcc + ca-certs. This removes the toolset an attacker
# would otherwise have inside the container after a code execution flaw.
# The delta_server binary is PID 1 directly; signal handling is built-in.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder /src/build/delta_server /usr/local/bin/delta_server
COPY --from=builder /src/LICENSE            /usr/local/share/delta/LICENSE

# nonroot user from distroless: uid 65532. /data must be a mounted
# writable volume — the operator is responsible for chown'ing it.
USER nonroot
WORKDIR /data
VOLUME ["/data"]

ENV HOME=/data

EXPOSE 16888 16889 16890

# distroless has no shell/nc, so the HEALTHCHECK is omitted. Operators
# should configure an external probe (e.g. k8s readinessProbe with
# httpGet on /healthz / port 16888).

ENTRYPOINT ["/usr/local/bin/delta_server"]
