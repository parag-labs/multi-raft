"""flotilla: run thousands of Raft groups on one set of nodes as a single fleet."""

from .cluster import Cluster
from .scheduler import GroupReplica, Scheduler

__all__ = ["Cluster", "Scheduler", "GroupReplica"]
__version__ = "0.1.0"
