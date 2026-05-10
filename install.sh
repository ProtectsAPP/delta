#!/usr/bin/env bash
# ----------------------------------------------------------------------
# Delta one-click installer.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/ProtectsAPP/delta/main/install.sh | bash
#   curl -fsSL .../install.sh | DELTA_DATA=~/data bash
#   curl -fsSL .../install.sh | DELTA_NO_START=1 bash
#
# Environment overrides:
#   DELTA_REPO        git URL              (default: https://github.com/ProtectsAPP/delta.git)
#   DELTA_REF         branch/tag/sha       (default: main)
#   DELTA_PREFIX      install root         (default: $HOME/.local/share/delta)
#   DELTA_BIN_DIR     where to symlink     (default: $HOME/.local/bin)
#   DELTA_DATA        data directory       (default: $HOME/.deltaql)
#   DELTA_PORT        listen port          (default: 16888)
#   DELTA_NO_START=1  skip auto-start
#   DELTA_NO_SERVICE=1 skip systemd/launchd registration
# ----------------------------------------------------------------------
set -euo pipefail

# -------- defaults ----------------------------------------------------
DELTA_REPO="${DELTA_REPO:-https://github.com/ProtectsAPP/delta.git}"
DELTA_REF="${DELTA_REF:-main}"
DELTA_PREFIX="${DELTA_PREFIX:-$HOME/.local/share/delta}"
DELTA_BIN_DIR="${DELTA_BIN_DIR:-$HOME/.local/bin}"
DELTA_DATA="${DELTA_DATA:-$HOME/.deltaql}"
DELTA_PORT="${DELTA_PORT:-16888}"

# -------- pretty print ------------------------------------------------
RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BLUE=$'\033[36m'; DIM=$'\033[2m'; BOLD=$'\033[1m'; OFF=$'\033[0m'
say()  { printf '%s%s%s %s\n' "$BLUE"  "==>" "$OFF" "$*"; }
ok()   { printf '%s%s%s %s\n' "$GREEN" " ✓ " "$OFF" "$*"; }
warn() { printf '%s%s%s %s\n' "$YELLOW" " ! " "$OFF" "$*"; }
fail() { printf '%s%s%s %s\n' "$RED"   " ✗ " "$OFF" "$*"; exit 1; }

banner() {
cat <<EOF

${BOLD}${GREEN}      Δ${OFF}  ${BOLD}Delta installer${OFF}    ${DIM}documents · cache · vectors · RBAC · replication${OFF}

EOF
}

# -------- prerequisite check -----------------------------------------
need() { command -v "$1" >/dev/null 2>&1 || fail "missing prerequisite: $1"; }

detect_os() {
    case "$(uname -s)" in
        Darwin) OS=mac   ;;
        Linux)  OS=linux ;;
        *)      fail "unsupported OS: $(uname -s)" ;;
    esac
}

ensure_deps() {
    say "checking build prerequisites..."
    need git
    need cmake
    need make
    if ! command -v cc >/dev/null && ! command -v clang >/dev/null && ! command -v gcc >/dev/null; then
        fail "no C/C++ compiler found (install Xcode CLT on mac, or build-essential on Linux)"
    fi
    ok "prerequisites OK"
}

# -------- fetch source -----------------------------------------------
fetch_source() {
    say "fetching ${DELTA_REPO}@${DELTA_REF} → ${DELTA_PREFIX}"
    if [[ -d "$DELTA_PREFIX/.git" ]]; then
        git -C "$DELTA_PREFIX" fetch --depth 1 origin "$DELTA_REF"
        git -C "$DELTA_PREFIX" reset --hard FETCH_HEAD
    else
        mkdir -p "$(dirname "$DELTA_PREFIX")"
        git clone --depth 1 --branch "$DELTA_REF" "$DELTA_REPO" "$DELTA_PREFIX"
    fi
    ok "source ready"
}

# -------- build ------------------------------------------------------
build_server() {
    say "compiling delta_server (this takes ~30s)..."
    local jobs
    jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    # wipe any stale CMakeCache that may have come along in the clone
    rm -rf "$DELTA_PREFIX/build/CMakeCache.txt" "$DELTA_PREFIX/build/CMakeFiles"
    cmake -S "$DELTA_PREFIX" -B "$DELTA_PREFIX/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
    cmake --build "$DELTA_PREFIX/build" --target delta_server -j "$jobs" >/dev/null
    [[ -x "$DELTA_PREFIX/build/delta_server" ]] || fail "build failed"
    ok "binary at $DELTA_PREFIX/build/delta_server ($(du -h "$DELTA_PREFIX/build/delta_server" | awk '{print $1}'))"
}

# -------- link into PATH ---------------------------------------------
link_bin() {
    mkdir -p "$DELTA_BIN_DIR"
    ln -sf "$DELTA_PREFIX/build/delta_server" "$DELTA_BIN_DIR/delta-server"
    ok "symlinked: $DELTA_BIN_DIR/delta-server"
    case ":$PATH:" in
        *":$DELTA_BIN_DIR:"*) ;;
        *) warn "$DELTA_BIN_DIR is not on PATH — add this to your shell rc:"
           printf '       export PATH="%s:$PATH"\n' "$DELTA_BIN_DIR" ;;
    esac
}

# -------- data directory ---------------------------------------------
prep_data() {
    mkdir -p "$DELTA_DATA"
    ok "data directory: $DELTA_DATA"
}

# -------- service / autostart ----------------------------------------
register_launchd() {
    local plist="$HOME/Library/LaunchAgents/com.delta.server.plist"
    mkdir -p "$(dirname "$plist")"
    cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>             <string>com.delta.server</string>
    <key>ProgramArguments</key>
    <array>
      <string>$DELTA_PREFIX/build/delta_server</string>
      <string>--data</string>   <string>$DELTA_DATA</string>
      <string>--port</string>   <string>$DELTA_PORT</string>
    </array>
    <key>RunAtLoad</key>         <true/>
    <key>KeepAlive</key>         <true/>
    <key>StandardOutPath</key>   <string>$DELTA_PREFIX/server.log</string>
    <key>StandardErrorPath</key> <string>$DELTA_PREFIX/server.log</string>
  </dict>
</plist>
PLIST
    launchctl unload "$plist" 2>/dev/null || true
    launchctl load   "$plist"
    ok "registered launchd agent → com.delta.server"
}

register_systemd() {
    local unit="$HOME/.config/systemd/user/delta.service"
    mkdir -p "$(dirname "$unit")"
    cat > "$unit" <<UNIT
[Unit]
Description=Delta multi-model database
After=network.target

[Service]
Type=simple
ExecStart=$DELTA_PREFIX/build/delta_server --data $DELTA_DATA --port $DELTA_PORT
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
UNIT
    if command -v systemctl >/dev/null && systemctl --user >/dev/null 2>&1; then
        systemctl --user daemon-reload
        systemctl --user enable --now delta.service
        ok "registered systemd user unit → delta.service"
    else
        warn "systemd --user not available; skipped service registration"
        warn "start manually with: $DELTA_PREFIX/build/delta_server --data $DELTA_DATA --port $DELTA_PORT"
    fi
}

start_server() {
    [[ "${DELTA_NO_START:-}" = "1" ]] && { ok "skipping start (DELTA_NO_START=1)"; return; }
    if [[ "${DELTA_NO_SERVICE:-}" = "1" ]]; then
        say "starting delta_server in background..."
        nohup "$DELTA_PREFIX/build/delta_server" --data "$DELTA_DATA" --port "$DELTA_PORT" \
              > "$DELTA_PREFIX/server.log" 2>&1 &
        disown
    else
        say "registering as a system service (auto-restart on crash / boot)..."
        if [[ "$OS" = "mac" ]]; then register_launchd; else register_systemd; fi
    fi
    sleep 2
    if curl -fsS "http://127.0.0.1:$DELTA_PORT/api/v1/health" >/dev/null; then
        ok "delta_server is live on http://127.0.0.1:$DELTA_PORT"
    else
        fail "delta_server didn't respond — see $DELTA_PREFIX/server.log"
    fi
}

# -------- post-install message ---------------------------------------
done_msg() {
cat <<EOF

${GREEN}${BOLD}  Δ  Delta is installed.${OFF}

  ${BOLD}Endpoint${OFF}      http://127.0.0.1:$DELTA_PORT
  ${BOLD}Data dir${OFF}      $DELTA_DATA
  ${BOLD}Binary${OFF}        $DELTA_PREFIX/build/delta_server
  ${BOLD}Service${OFF}       $( [[ "$OS" = mac ]] && echo "launchd · com.delta.server" || echo "systemd --user · delta.service" )
  ${BOLD}Default login${OFF} admin / admin   ${DIM}(change immediately!)${OFF}

  ${DIM}# quick smoke test${OFF}
  curl -s http://127.0.0.1:$DELTA_PORT/api/v1/health
  curl -s -X POST http://127.0.0.1:$DELTA_PORT/api/v1/auth/login \\
       -H 'Content-Type: application/json' \\
       -d '{"username":"admin","password":"admin"}'

  Docs:    https://github.com/ProtectsAPP/delta
  Console: cd $DELTA_PREFIX/console && cnpm install && cnpm run dev

EOF
}

# ----------------------------------------------------------------------
banner
detect_os
ensure_deps
fetch_source
build_server
link_bin
prep_data
start_server
done_msg
