import logging

from csv2pg.main import copy_to


name = "csv2pg"
__version__ = "2.2.0"

logging.basicConfig()
logging.getLogger("csv2pg").setLevel(logging.WARNING)

__all__ = ["copy_to"]
