import logging
logger = logging.getLogger(__name__)


def detect_environment():
    import os
    import sys
    if "COLAB_RELEASE_TAG" in os.environ:
        return "colab"
    elif "RENDER" in os.environ:
        return "render"
    elif sys.platform.startswith("win"):
        return "windows"
    elif sys.platform.startswith("linux"):
        return "linux"


def resolve_secret(value):
    if value.startswith("$"):
        import os
        return os.getenv(value[1:])
    return value
