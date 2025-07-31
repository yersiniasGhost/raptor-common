from typing import Union, List, Dict
from enum import Enum

JSON = Union[List['JSON'],
             Dict[str, 'JSON'],
             str,
             float,
             bool,
             None]


class DeploymentType(Enum):
    stage = "STAGE"
    production = "PRODUCTION"
    development = "DEVEL"

