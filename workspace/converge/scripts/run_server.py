"""Run the convergence HTTP service."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.web.api import main  # noqa: E402

if __name__ == "__main__":
    main()
