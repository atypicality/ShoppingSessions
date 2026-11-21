import json
import os
import subprocess
import sys
from pathlib import Path
import duckdb

# the main idea here is to be able to take special parameters for testing
# this now supports CLI arguments (for local testing) and environment variables (for Docker):
def copy_csv_to_parquet(con: duckdb.DuckDBPyConnection, source: Path, destination: Path, columns: str, select_list: str) -> None:
    con.execute(f"""
        COPY (
            SELECT {select_list}
            FROM read_csv(
                '{source.as_posix()}',
                header=true,
                delim=',',
                quote='"',
                columns={{{columns}}}
            )
        ) TO '{destination.as_posix()}' (FORMAT PARQUET);
    """)


def main() -> None:
    if len(sys.argv) >= 3:
        data_dir = Path(sys.argv[1])
        output_dir = Path(sys.argv[2])
    else:
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        output_dir = Path(os.environ.get("OUTPUT_DIR", "/output"))

    scratch_dir = Path(os.environ.get("SCRATCH_DIR", "/tmp/scratch"))
    scratch_dir.mkdir(parents=True, exist_ok=True)

    # Read manifest.json in Python so C++ doesn't need a JSON parser
    manifest = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))
    window_start = manifest["window_start"]
    window_end = manifest["window_end"]

    # Locate and run the compiled C++ binary
    repo_root = Path(__file__).resolve().parent.parent
    cpp_bin = repo_root / "cpp" / "pipeline"
    user_csv = scratch_dir / "user_daily.csv"
    campaign_csv = scratch_dir / "campaign_daily.csv"

    cmd = [
        str(cpp_bin),
        str(data_dir),
        window_start,
        window_end,
        str(user_csv),
        str(campaign_csv),
    ]
    subprocess.run(cmd, check=True)

    # Prepare clean output directories
    user_out = output_dir / "user_daily"
    campaign_out = output_dir / "campaign_daily"
    user_out.mkdir(parents=True, exist_ok=True)
    campaign_out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        copy_csv_to_parquet(
            con, user_csv, user_out / "part-0.parquet",
            "'user_id': 'VARCHAR', 'day': 'DATE', 'sessions': 'BIGINT', 'events': 'BIGINT', 'revenue_cents': 'BIGINT', 'top_category': 'VARCHAR'",
            """user_id, day, sessions, events, revenue_cents, CASE WHEN top_category = '' THEN NULL ELSE top_category END AS top_category""",
        )
        copy_csv_to_parquet(
            con, campaign_csv, campaign_out / "part-0.parquet",
            "'campaign_id': 'VARCHAR', 'channel': 'VARCHAR', 'day': 'DATE', 'sessions': 'BIGINT', 'purchasing_sessions': 'BIGINT', 'revenue_cents': 'BIGINT', 'assisted_sessions': 'BIGINT'",
            """campaign_id, channel, day, sessions, purchasing_sessions, revenue_cents, assisted_sessions AS assisted_sessions""",
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
