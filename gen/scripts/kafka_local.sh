#!/usr/bin/env bash
# Single-node Apache Kafka (KRaft) on localhost:9092 for local tests, without Docker.
#
#   scripts/kafka_local.sh start    download (once, sha512-verified), format fresh storage, start in background
#   scripts/kafka_local.sh stop     stop the broker started by this script (by its recorded PID only)
#   scripts/kafka_local.sh status
#
# Everything lives in ${VQ_KAFKA_HOME:-~/.cache/vq-kafka}: the unpacked release, kafka.pid, and a fresh
# kraft-XXXXXX data dir per start (removed on stop). Needs Java 17+ and curl.
set -euo pipefail

KAFKA_VERSION=3.9.1
SCALA_VERSION=2.13
NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${NAME}.tgz"
HOME_DIR="${VQ_KAFKA_HOME:-$HOME/.cache/vq-kafka}"
KAFKA_DIR="$HOME_DIR/$NAME"
PID_FILE="$HOME_DIR/kafka.pid"
DATA_FILE="$HOME_DIR/kafka.datadir"
BROKER_PORT=9092
CONTROLLER_PORT=9093
START_TIMEOUT_S=60
STOP_TIMEOUT_S=30

log() { echo "kafka_local: $*" >&2; }
die() { log "error: $*"; exit 1; }

port_in_use() { [ -n "$(ss -ltnH "sport = :$1" 2>/dev/null)" ]; }

# PID of our broker if it is alive and really is the Kafka JVM we started (guards against PID reuse).
our_pid() {
    [ -f "$PID_FILE" ] || return 1
    local pid datadir
    pid=$(cat "$PID_FILE")
    datadir=$(cat "$DATA_FILE" 2>/dev/null || true)
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    [ -r "/proc/$pid/cmdline" ] || return 1
    local cmd
    cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline")
    [[ "$cmd" == *kafka.Kafka* && -n "$datadir" && "$cmd" == *"$datadir/server.properties"* ]] || return 1
    echo "$pid"
}

download() {
    [ -x "$KAFKA_DIR/bin/kafka-server-start.sh" ] && return 0
    command -v java >/dev/null || die "java not found (Kafka $KAFKA_VERSION needs Java 17+)"
    mkdir -p "$HOME_DIR"
    local tmp
    tmp=$(mktemp -d "$HOME_DIR/download-XXXXXX")
    trap "rm -rf '$tmp'" EXIT
    log "downloading $URL"
    curl -fsSL --retry 3 -o "$tmp/$NAME.tgz" "$URL" || die "download failed: $URL"
    curl -fsSL --retry 3 -o "$tmp/$NAME.tgz.sha512" "$URL.sha512" || die "download failed: $URL.sha512"
    # Apache format: "<file>: HEX HEX ..." spread over several lines
    local want got
    want=$(cut -d: -f2- "$tmp/$NAME.tgz.sha512" | tr -d ' \n\r\t' | tr 'A-F' 'a-f')
    got=$(sha512sum "$tmp/$NAME.tgz" | cut -d' ' -f1)
    [ "$want" = "$got" ] || die "sha512 mismatch for $NAME.tgz"
    mkdir "$tmp/x"
    tar -xzf "$tmp/$NAME.tgz" -C "$tmp/x" --no-same-owner
    [ -x "$tmp/x/$NAME/bin/kafka-server-start.sh" ] || die "unexpected archive layout"
    mv "$tmp/x/$NAME" "$KAFKA_DIR"
    rm -rf "$tmp"
    trap - EXIT
    log "installed $KAFKA_DIR"
}

start() {
    if pid=$(our_pid); then
        log "already running (pid $pid) on localhost:$BROKER_PORT"
        return 0
    fi
    for p in $BROKER_PORT $CONTROLLER_PORT; do
        port_in_use "$p" && die "port $p is already in use by another process"
    done
    download
    rm -f "$PID_FILE"
    local old
    old=$(cat "$DATA_FILE" 2>/dev/null || true)
    [[ -n "$old" && "$old" == "$HOME_DIR"/kraft-* ]] && rm -rf "$old"

    local data
    data=$(mktemp -d "$HOME_DIR/kraft-XXXXXX")
    echo "$data" > "$DATA_FILE"
    cat > "$data/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:$CONTROLLER_PORT
listeners=PLAINTEXT://localhost:$BROKER_PORT,CONTROLLER://localhost:$CONTROLLER_PORT
advertised.listeners=PLAINTEXT://localhost:$BROKER_PORT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$data/logs
num.partitions=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.retention.hours=24
EOF
    local cluster_id
    cluster_id=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
    "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$cluster_id" -c "$data/server.properties" >"$data/format.log" 2>&1 \
        || { cat "$data/format.log" >&2; die "storage format failed"; }

    # kafka-server-start.sh execs kafka-run-class.sh which execs java, so $! is the JVM's PID.
    KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}" LOG_DIR="$data/app-logs" \
        nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$data/server.properties" >"$data/kafka.out" 2>&1 </dev/null &
    local pid=$!
    echo "$pid" > "$PID_FILE"

    local waited=0
    until port_in_use "$BROKER_PORT"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            tail -n 30 "$data/kafka.out" >&2 || true
            rm -f "$PID_FILE"
            die "broker exited during startup (log: $data/kafka.out)"
        fi
        if [ "$waited" -ge "$START_TIMEOUT_S" ]; then
            tail -n 30 "$data/kafka.out" >&2 || true
            die "broker did not listen on $BROKER_PORT within ${START_TIMEOUT_S}s (pid $pid still running; use stop)"
        fi
        sleep 1
        waited=$((waited + 1))
    done
    our_pid >/dev/null || die "pid $pid does not look like the Kafka JVM started here"
    log "started Kafka $KAFKA_VERSION (pid $pid) on localhost:$BROKER_PORT, data $data"
    echo "export KAFKA_BOOTSTRAP=localhost:$BROKER_PORT"
}

stop() {
    local pid
    if ! pid=$(our_pid); then
        log "not running (no live broker recorded in $PID_FILE)"
        rm -f "$PID_FILE"
        return 0
    fi
    log "stopping pid $pid"
    kill -TERM "$pid"
    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge "$STOP_TIMEOUT_S" ]; then
            log "pid $pid still alive after ${STOP_TIMEOUT_S}s, sending SIGKILL"
            kill -KILL "$pid" 2>/dev/null || true
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done
    rm -f "$PID_FILE"
    local data
    data=$(cat "$DATA_FILE" 2>/dev/null || true)
    if [[ -n "$data" && "$data" == "$HOME_DIR"/kraft-* ]]; then
        rm -rf "$data"
        rm -f "$DATA_FILE"
    fi
    log "stopped"
}

status() {
    local pid
    if pid=$(our_pid); then
        if port_in_use "$BROKER_PORT"; then
            log "running (pid $pid) on localhost:$BROKER_PORT"
        else
            log "process $pid alive but not listening on $BROKER_PORT yet"
        fi
    else
        log "not running"
        return 3
    fi
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    status) status ;;
    *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
