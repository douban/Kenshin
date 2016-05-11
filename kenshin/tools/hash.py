# coding: utf-8
import fnv1a

class Hash:
    def __init__(self, nodes):
        self.nodes = nodes

    def add_node(self, node):
        self.nodes.append(node)

    def remove_code(self, node):
        self.nodes.remove(node)

    def get_node(self, key):
        idx = fnv1a.get_hash_bugfree(key) % len(self.nodes)
        return self.nodes[idx]

    def get_nodes(self, key):
        idx = fnv1a.get_hash_bugfree(key) % len(self.nodes)
        return self.nodes[idx:] + self.nodes[:idx]
