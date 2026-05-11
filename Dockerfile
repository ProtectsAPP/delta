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
FROM debian:bookworm-slim AS runtime

# libstdc++ + libgcc only; nothing else (no shell tooling, no package manager
# leftovers). Run as a non-root user.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        libstdc++6 libgcc-s1 ca-certificates tini netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r delta && useradd -r -g delta -d /data -s /usr/sbin/nologin delta \
    && mkdir -p /data && chown -R delta:delta /data

COPY --from=builder /src/build/delta_server /usr/local/bin/delta_server
COPY --from=builder /src/LICENSE            /usr/local/share/delta/LICENSE

USER delta
WORKDIR /data
VOLUME ["/data"]

# Inside the container, ~/.deltaql == /data thanks to the user's $HOME.
ENV HOME=/data

EXPOSE 16888 16889 16890

# Light TCP health-check on the HTTP port — the REST listener is always on.
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z 127.0.0.1 16888 || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/delta_server"]
