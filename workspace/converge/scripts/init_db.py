"""Initialize the convergence database and built-in demo accounts."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import db_path  # noqa: E402
from app.store import connect, init
from app.web.api import init_state


def main() -> int:
    path = db_path()
    conn = connect(path)
    init(conn)
    state = init_state(path)
    print(json.dumps({"db": path, "recovery": state["recover"]}, ensure_ascii=False, indent=2))
    print(
        "accounts:\n"
        "  operator / operator-pw   (上报信封、查看图谱)\n"
        "  reviewer-a / review-a-pw (第一复核员)\n"
        "  reviewer-b / review-b-pw (第二复核员)\n"
        "  inspector / inspector-pw (只读查询/下载脱敏收据)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
