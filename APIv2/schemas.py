from typing import List, Union

from pydantic import BaseModel


class ClusterBase(BaseModel):
    clusterId: int


class ClusterRelationsPost(ClusterBase):
    nodeClusters: List[int]

