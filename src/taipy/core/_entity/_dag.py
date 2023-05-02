# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
from math import lcm
from typing import Dict, List, Tuple

import networkx as nx

from src.taipy.core._entity._entity import _Entity


class _Node:
    def __init__(self, entity: _Entity, x, y):
        self.type = entity.__class__.__name__
        self.entity = entity
        self.x = x
        self.y = y


class _Edge:
    def __init__(self, src: _Node, dest: _Node):
        self.src = src
        self.dest = dest


class _DAG:
    def __init__(self, dag: nx.DiGraph):
        self._sorted_nodes = list(nodes for nodes in nx.topological_generations(dag))
        self._length, self._width = self.__compute_size()
        self._grid_length, self._grid_width = self.__compute_grid_size()
        self._nodes = self.__compute_nodes()
        self._edges = self.__compute_edges(dag)

    @property
    def width(self) -> int:
        return self._width

    @property
    def length(self) -> int:
        return self._length

    @property
    def nodes(self) -> Dict[str, _Node]:
        return self._nodes

    @property
    def edges(self) -> List[_Edge]:
        return self._edges

    def __compute_size(self) -> Tuple[int, int]:
        return len(self._sorted_nodes), max([len(i) for i in self._sorted_nodes])

    def __compute_grid_size(self) -> Tuple[int, int]:
        grid_width = lcm(*[len(i) + 1 if len(i) != self._width else len(i) - 1 for i in self._sorted_nodes]) + 1
        return len(self._sorted_nodes), grid_width

    def __compute_nodes(self) -> Dict[str, _Node]:
        nodes = {}
        x = 0
        for same_lvl_nodes in self._sorted_nodes:
            local_width = len(same_lvl_nodes)
            is_max = local_width != self.width
            y_incr = (
                (self._grid_width - 1) / (local_width + 1) if is_max else (self._grid_width - 1) / (local_width - 1)
            )
            y = 0 if is_max else -y_incr
            for node in same_lvl_nodes:
                y += y_incr
                nodes[node.id] = _Node(node, x, y)
            x += 1
        return nodes

    def __compute_edges(self, dag) -> List[_Edge]:
        edges = []
        for edge in dag.edges():
            edges.append(_Edge(self.nodes[edge[0].id], self.nodes[edge[1].id]))
        return edges
