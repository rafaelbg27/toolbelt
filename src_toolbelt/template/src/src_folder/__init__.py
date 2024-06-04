import os

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def get_data_path(path):
    return os.path.join(_ROOT, "data", path)


def get_queries_path(path):
    return os.path.join(_ROOT, "data/queries", path)


def get_lib_path(path):
    return os.path.join(_ROOT, "lib", path)


def get_models_path(path):
    return os.path.join(_ROOT, "models", path)


def get_docs_path(path):
    return os.path.join(_ROOT, "docs", path)


def get_reports_path(path):
    return os.path.join(_ROOT, "reports", path)
