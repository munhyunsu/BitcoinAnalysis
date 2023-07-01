from typing import List, Union

from pydantic import BaseModel


class ClusterBase(BaseModel):
    clusterId: int = 860103337


class EdgeBase(BaseModel):
    nodeClusters: List[int] = [519495737]


class ClusterRelationsPost(ClusterBase, EdgeBase):
    pass


class EdgeSelectPost(EdgeBase):
    nodeClusters: List[int] = [860103337, 519495737]
