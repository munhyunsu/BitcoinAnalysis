from typing import List, Union

from pydantic import BaseModel


class ClusterBase(BaseModel):
    clusterId: int = 860103337


class EdgeBase(BaseModel):
    nodeClusters: List[int] = [519495737]


class TransactionBase(BaseModel):
    transferTxhash: str = '0aacf25d562d9078cfd79118bda4acb84aae83d634c941a933e782719a9be749'


class ClusterRelationsPost(ClusterBase, EdgeBase):
    pass


class EdgeSelectPost(EdgeBase):
    nodeClusters: List[int] = [860103337, 519495737]


class ClusterInfoPost(ClusterBase):
    pass


class TransferInfoPost(ClusterBase):
    pass


class DetailTransferPost(ClusterBase, TransactionBase):
    pass
