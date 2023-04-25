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

from typing import Iterable

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
    def __init__(self, dag: nx.DiGraph, x_increment=500, y_max=1000):
        self.width = 0
        self.length = 0
        self.sorted_nodes = list(nodes for nodes in nx.topological_generations(dag))
        self.nodes = self.__get_nodes_and_compute_width_length(x_increment, y_max)
        self.edges = self.__get_edges(dag)

    def __get_nodes_and_compute_width_length(self, x_increment, y_max) -> dict[str, _Node]:
        nodes = {}
        x = 0
        for same_lvl_nodes in self.sorted_nodes:
            local_width = len(same_lvl_nodes)
            if local_width > self.width:
                self.width = local_width
            x += x_increment
            y_incr = y_max / (local_width + 1)
            y = 0
            for node in same_lvl_nodes:
                y += y_incr
                nodes[node.id] = _Node(node, x, y)
        self.length = x / x_increment
        return nodes

    def __get_edges(self, dag) -> Iterable[_Edge]:
        edges = []
        for edge in dag.edges():
            edges.append(_Edge(self.nodes[edge[0].id], self.nodes[edge[1].id]))
        return edges
