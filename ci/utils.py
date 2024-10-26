import os
import logging
from subprocess import PIPE, CalledProcessError, Popen
import sys

import termcolor


logger = logging.getLogger(__name__)


def setup_logging():
    kw = {
        "format": "[%(asctime)s][%(module)s]: %(message)s",
        "datefmt": "%m/%d/%Y %H:%M:%S",
        "level": logging.DEBUG,
        "stream": sys.stdout,
    }

    logging.basicConfig(**kw)
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
        logging.WARNING
    )


def shell(cmd, inputdata=None, wait=True, **kw):
    info('calling "%s" in %s', cmd, kw.get("cwd", os.getcwd()))
    kw["shell"] = not isinstance(cmd, list)
    kw["stdin"] = PIPE if inputdata else None
    p = Popen(cmd, **kw)
    if inputdata:
        p.communicate(inputdata)
    if wait:
        p.wait()
        if p.returncode:
            raise CalledProcessError(p.returncode, cmd)
    else:
        return p


def info(fmt, *a):
    logger.info(green(fmt), *a)


def green(s):
    return _color(s, "green")


def _color(s, color):
    return s if not os.isatty(sys.stdout.fileno()) else termcolor.colored(str(s), color)
