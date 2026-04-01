import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from etl.incremental_load import run_incremental_load

if __name__ == "__main__":
    result = run_incremental_load()

    print("\n============================================================")
    print("INCREMENTAL LOAD RESULT")
    print("============================================================")
    print(f"Rows read:    {result['rows_read']}")
    print(f"Rows written: {result['rows_written']}")
    print(f"Duration:     {result['duration_sec']} sec")
    print("============================================================")