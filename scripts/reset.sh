#!/usr/bin/env bash
# Reset ONLY this project's data: drops and recreates the project's tables
# (transactions, dataset_metadata) in the project's database, then reloads.
# Does not touch other databases, containers or volumes.
#   scripts/reset.sh                 # asks for confirmation, reloads the default dataset
#   scripts/reset.sh -y              # no prompt
#   scripts/reset.sh -y --empty      # leave the tables empty
#   scripts/reset.sh -y --count 100000 --seed 7   # reload with other loader args
set -euo pipefail
cd "$(dirname "$0")/.."

yes=0; empty=0; loader_args=()
for a in "$@"; do
  case "$a" in
    -y|--yes) yes=1 ;;
    --empty) empty=1 ;;
    *) loader_args+=("$a") ;;
  esac
done

set -a; source .env; set +a
if [[ $yes -ne 1 ]]; then
  read -r -p "Drop and recreate tables in database '${POSTGRES_DB}' (project container only)? [y/N] " ans
  [[ "$ans" =~ ^[Yy]$ ]] || { echo "aborted"; exit 1; }
fi

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "DROP TABLE IF EXISTS transactions, dataset_metadata;"
docker compose exec -T postgres psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < sql/schema.sql
echo "tables recreated (empty)"

if [[ $empty -eq 0 ]]; then
  .venv/bin/python scripts/load_data.py --force ${loader_args[@]+"${loader_args[@]}"}
fi
