#!/usr/bin/env bash
set -euo pipefail

remote_spec=""
local_root=""
ssh_port=""
identity_file=""
bwlimit=""
python_exe=""
mode="archive"
dry_run=0
compress=1
verbose=0
compressed_only=0
ack_delete_finalized=1
includes=()

default_includes=(
  "all_market_meta.json"
  "latest_markets.json"
  "books"
  "price_changes"
  "resolved_market"
  "raw"
  "warehouse"
)

compressed_only_includes=(
  "all_market_meta.json"
  "latest_markets.json"
  "resolved_market"
  "raw"
  "warehouse"
  "finalized"
)

usage() {
  cat <<'EOF'
Usage: pull_cloud_data.sh --remote user@host:/abs/remote/data --local-root /abs/local/data [options]

Required:
  --remote SPEC               Remote source in rsync/ssh form, e.g. user@host:/srv/polymarket-data
  --local-root PATH           Local destination root

Options:
  --include PATH              Relative path under data root to pull. Repeatable.
  --mode archive|mirror       Archive keeps local-only files. Mirror deletes local files missing on remote.
  --ssh-port PORT             SSH port
  --identity-file PATH        SSH private key
  --bwlimit KBPS              Limit rsync bandwidth in KB/s
  --compressed-only           Only pull sealed compressed outputs and metadata
  --no-ack-delete-finalized   Keep remote finalized/*.jsonl.gz after pull (default deletes with ack)
  --dry-run                   Show planned transfers only
  --no-compress               Disable rsync transport compression
  --verbose                   Print each rsync command
  -h, --help                  Show this help

Defaults:
  include = all_market_meta.json, latest_markets.json, books, price_changes, resolved_market, raw, warehouse
  mode = archive
EOF
}

quote_for_sh() {
  local value="$1"
  value=${value//\'/\'\\\'\'}
  printf "'%s'" "$value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remote)
      remote_spec="$2"
      shift 2
      ;;
    --local-root)
      local_root="$2"
      shift 2
      ;;
    --include)
      includes+=("$2")
      shift 2
      ;;
    --mode)
      mode="$2"
      shift 2
      ;;
    --ssh-port)
      ssh_port="$2"
      shift 2
      ;;
    --identity-file)
      identity_file="$2"
      shift 2
      ;;
    --bwlimit)
      bwlimit="$2"
      shift 2
      ;;
    --compressed-only)
      compressed_only=1
      shift
      ;;
    --no-ack-delete-finalized)
      ack_delete_finalized=0
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    --no-compress)
      compress=0
      shift
      ;;
    --verbose)
      verbose=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$remote_spec" || -z "$local_root" ]]; then
  usage >&2
  exit 1
fi

if [[ "$mode" != "archive" && "$mode" != "mirror" ]]; then
  echo "--mode must be archive or mirror" >&2
  exit 1
fi

if [[ "${#includes[@]}" -eq 0 ]]; then
  if [[ $compressed_only -eq 1 ]]; then
    includes=("${compressed_only_includes[@]}")
  else
    includes=("${default_includes[@]}")
  fi
fi

if [[ "$remote_spec" != *:* ]]; then
  echo "--remote must look like user@host:/absolute/path" >&2
  exit 1
fi

remote_host="${remote_spec%%:*}"
remote_root="${remote_spec#*:}"
if [[ -z "$remote_host" || -z "$remote_root" ]]; then
  echo "Invalid --remote value: $remote_spec" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

mkdir -p "$local_root"
transfer_state_dir="$local_root/state/transfers"
mkdir -p "$transfer_state_dir"

ssh_args=()
if [[ -n "$ssh_port" ]]; then
  ssh_args+=("-p" "$ssh_port")
fi
if [[ -n "$identity_file" ]]; then
  ssh_args+=("-i" "$identity_file")
fi

ssh_cmd=("ssh" "${ssh_args[@]}")
rsync_args=(-a --human-readable --partial)
if [[ $compress -eq 1 ]]; then
  rsync_args+=(-z)
fi
if [[ -n "$bwlimit" ]]; then
  rsync_args+=("--bwlimit=$bwlimit")
fi
if [[ $dry_run -eq 1 ]]; then
  rsync_args+=(--dry-run --itemize-changes)
fi
if [[ $verbose -eq 1 ]]; then
  rsync_args+=(-v)
fi
if [[ "$mode" == "mirror" ]]; then
  rsync_args+=(--delete-delay)
fi

ssh_rsync_cmd="ssh"
for arg in "${ssh_args[@]}"; do
  ssh_rsync_cmd+=" $(printf '%q' "$arg")"
done

pulled_items=()
missing_items=()
ack_marked_count=0
remote_deleted_count=0
remote_missing_count=0
remote_size_mismatch_count=0
ts_now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

for item in "${includes[@]}"; do
  normalized_item="${item#/}"
  remote_item_path="${remote_root%/}/$normalized_item"
  quoted_remote_item_path="$(quote_for_sh "$remote_item_path")"

  if "${ssh_cmd[@]}" "$remote_host" "test -d $quoted_remote_item_path"; then
    mkdir -p "$local_root/$normalized_item"
    cmd=(
      rsync
      "${rsync_args[@]}"
      -e "$ssh_rsync_cmd"
      "${remote_host}:${remote_item_path%/}/"
      "$local_root/$normalized_item/"
    )
    if [[ $verbose -eq 1 ]]; then
      printf 'Running:'
      printf ' %q' "${cmd[@]}"
      printf '\n'
    fi
    "${cmd[@]}"
    pulled_items+=("$normalized_item/")
    continue
  fi

  if "${ssh_cmd[@]}" "$remote_host" "test -e $quoted_remote_item_path"; then
    mkdir -p "$(dirname "$local_root/$normalized_item")"
    cmd=(
      rsync
      "${rsync_args[@]}"
      -e "$ssh_rsync_cmd"
      "${remote_host}:${remote_item_path}"
      "$local_root/$normalized_item"
    )
    if [[ $verbose -eq 1 ]]; then
      printf 'Running:'
      printf ' %q' "${cmd[@]}"
      printf '\n'
    fi
    "${cmd[@]}"
    pulled_items+=("$normalized_item")
    continue
  fi

  missing_items+=("$normalized_item")
done

has_finalized_include=0
for item in "${includes[@]}"; do
  if [[ "${item#/}" == "finalized" ]]; then
    has_finalized_include=1
    break
  fi
done

if [[ $dry_run -eq 0 && $ack_delete_finalized -eq 1 && $has_finalized_include -eq 1 ]]; then
  finalized_root="$local_root/finalized"
  if [[ -d "$finalized_root" ]]; then
    manifest_path="$(mktemp)"
    find "$finalized_root" -type f -name "*.jsonl.gz" -printf '%P\t%s\n' >"$manifest_path"
    if [[ -s "$manifest_path" ]]; then
      local_ack_host="$(hostname -f 2>/dev/null || hostname)"
      remote_cleanup_script='
set -euo pipefail
remote_root="$1"
ack_ts="$2"
ack_host="$3"
ack_marked=0
remote_deleted=0
remote_missing=0
remote_size_mismatch=0
while IFS=$'\''\t'\'' read -r rel expected_size; do
  [[ -n "$rel" ]] || continue
  remote_file="$remote_root/finalized/$rel"
  if [[ ! -f "$remote_file" ]]; then
    remote_missing=$((remote_missing + 1))
    continue
  fi
  actual_size="$(stat -c %s "$remote_file" 2>/dev/null || echo -1)"
  if [[ "$actual_size" != "$expected_size" ]]; then
    remote_size_mismatch=$((remote_size_mismatch + 1))
    continue
  fi
  ack_file="$remote_root/state/transfers/acks/finalized/${rel}.ack.json"
  mkdir -p "$(dirname "$ack_file")"
  printf "{\\"acked_at_utc\\":\\"%s\\",\\"ack_host\\":\\"%s\\",\\"remote_file\\":\\"%s\\",\\"size_bytes\\":%s}\\n" \
    "$ack_ts" "$ack_host" "$remote_file" "$actual_size" >"$ack_file"
  rm -f "$remote_file"
  ack_marked=$((ack_marked + 1))
  remote_deleted=$((remote_deleted + 1))
done
printf "ack_marked=%d remote_deleted=%d remote_missing=%d remote_size_mismatch=%d\\n" \
  "$ack_marked" "$remote_deleted" "$remote_missing" "$remote_size_mismatch"
'
      remote_cleanup_result="$(
        "${ssh_cmd[@]}" "$remote_host" \
          "bash -c $(printf '%q' "$remote_cleanup_script") -- \
            $(printf '%q' "$remote_root") \
            $(printf '%q' "$ts_now") \
            $(printf '%q' "$local_ack_host")" <"$manifest_path"
      )"
      rm -f "$manifest_path"
      for token in $remote_cleanup_result; do
        case "$token" in
          ack_marked=*) ack_marked_count="${token#ack_marked=}" ;;
          remote_deleted=*) remote_deleted_count="${token#remote_deleted=}" ;;
          remote_missing=*) remote_missing_count="${token#remote_missing=}" ;;
          remote_size_mismatch=*) remote_size_mismatch_count="${token#remote_size_mismatch=}" ;;
        esac
      done
    else
      rm -f "$manifest_path"
    fi
  fi
fi

summary_path="$transfer_state_dir/pull_cloud_data_latest.json"

pulled_json="[]"
if [[ ${#pulled_items[@]} -gt 0 ]]; then
  pulled_json="["
  for item in "${pulled_items[@]}"; do
    if [[ "$pulled_json" != "[" ]]; then
      pulled_json+=","
    fi
    pulled_json+="\"$item\""
  done
  pulled_json+="]"
fi

missing_json="[]"
if [[ ${#missing_items[@]} -gt 0 ]]; then
  missing_json="["
  for item in "${missing_items[@]}"; do
    if [[ "$missing_json" != "[" ]]; then
      missing_json+=","
    fi
    missing_json+="\"$item\""
  done
  missing_json+="]"
fi

cat >"$summary_path" <<EOF
{
  "ts_utc": "$ts_now",
  "remote": "$remote_spec",
  "local_root": "$local_root",
  "mode": "$mode",
  "compressed_only": $compressed_only,
  "dry_run": $dry_run,
  "compress": $compress,
  "ack_delete_finalized": $ack_delete_finalized,
  "pulled_items": $pulled_json,
  "missing_items": $missing_json,
  "ack_marked": $ack_marked_count,
  "remote_deleted": $remote_deleted_count,
  "remote_missing": $remote_missing_count,
  "remote_size_mismatch": $remote_size_mismatch_count
}
EOF

cat "$summary_path"
