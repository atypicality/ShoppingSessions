import json
import os
import subprocess
import sys
from pathlib import Path
import duckdb


def main() -> None:
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

    # Convert the two aggregated CSVs to Parquet with exact schemas
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT
                user_id::VARCHAR AS user_id,
                day::DATE AS day,
                sessions::BIGINT AS sessions,
                events::BIGINT AS events,
                revenue_cents::BIGINT AS revenue_cents,
                CASE WHEN top_category = '' THEN NULL ELSE top_category END::VARCHAR AS top_category
            FROM read_csv(
                '{user_csv.as_posix()}',
                header=true,
                delim=',',
                quote='"',
                nullstr='',
                columns={{
                    'user_id': 'VARCHAR',
                    'day': 'VARCHAR',
                    'sessions': 'BIGINT',
                    'events': 'BIGINT',
                    'revenue_cents': 'BIGINT',
                    'top_category': 'VARCHAR'
                }}
            )
        ) TO '{(user_out / "part-0.parquet").as_posix()}' (FORMAT PARQUET);
    """)

    con.execute(f"""
        COPY (
            SELECT
                campaign_id::VARCHAR AS campaign_id,
                channel::VARCHAR AS channel,
                day::DATE AS day,
                sessions::BIGINT AS sessions,
                purchasing_sessions::BIGINT AS purchasing_sessions,
                revenue_cents::BIGINT AS revenue_cents,
                assisted_sessions::BIGINT AS assisted_sessions
            FROM read_csv(
                '{campaign_csv.as_posix()}',
                header=true,
                delim=',',
                quote='"',
                columns={{
                    'campaign_id': 'VARCHAR',
                    'channel': 'VARCHAR',
                    'day': 'VARCHAR',
                    'sessions': 'BIGINT',
                    'purchasing_sessions': 'BIGINT',
                    'revenue_cents': 'BIGINT',
                    'assisted_sessions': 'BIGINT'
                }}
            )
        ) TO '{(campaign_out / "part-0.parquet").as_posix()}' (FORMAT PARQUET);
    """)


if __name__ == "__main__":
    main()