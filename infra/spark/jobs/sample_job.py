import json
from datetime import datetime, timezone


def main():
    result = {
        "job": "sample_job",
        "status": "success",
        "records_processed": 1284920,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
