import os


def home() -> str:
    return os.environ.get("CONVERGE_HOME", os.path.join(os.getcwd(), "data"))


def db_path() -> str:
    return os.path.join(home(), "convergence.db")
