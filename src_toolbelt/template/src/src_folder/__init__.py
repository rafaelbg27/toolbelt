import os

import seaborn as sns

_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def get_data_path(path):
    return os.path.join(_ROOT, 'data', path)


def get_queries_path(path):
    return os.path.join(_ROOT, 'data/queries', path)


def get_lib_path(path):
    return os.path.join(_ROOT, 'lib', path)


def get_models_path(path):
    return os.path.join(_ROOT, 'models', path)


def get_docs_path(path):
    return os.path.join(_ROOT, 'docs', path)


def get_reports_path(path):
    return os.path.join(_ROOT, 'reports', path)


def get_kmp_onedrive_path(path):

    def _find_root_kmp_onedrive_path():
        folder_name = 'documentos - kamaroopin'
        dir_tree = os.walk(os.path.expanduser('~'))
        for dirpath, dirnames, filenames in dir_tree:
            for dirname in dirnames:
                if dirname.lower() == folder_name:
                    print('Found: {}'.format(os.path.join(dirpath, dirname)))
                    return os.path.join(_ROOT, dirpath, dirname)
        return None

    return os.path.join(_find_root_kmp_onedrive_path(),
                        'Business intelligence', path)


def get_kmp_onedrive_data_path(path):
    return os.path.join(get_kmp_onedrive_path('01. Datasets'), path)


def get_color_palette(size: int, name: str = 'husl'):
    return sns.color_palette(name, size).as_hex().tolist()
