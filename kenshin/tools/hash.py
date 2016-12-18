# coding: utf-8
from rurouni.fnv1a import get_int32_hash

class Hash:
    def __init__(self, nodes):
        self.nodes = nodes

    def add_node(self, node):
        self.nodes.append(node)

    def remove_code(self, node):
        self.nodes.remove(node)

    def get_node(self, key):
        idx = get_int32_hash(key) % len(self.nodes)
        return self.nodes[idx]

    def get_nodes(self, key):
        idx = get_int32_hash(key) % len(self.nodes)
        return self.nodes[idx:] + self.nodes[:idx]
