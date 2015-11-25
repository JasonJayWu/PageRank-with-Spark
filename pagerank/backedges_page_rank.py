from simple_page_rank import SimplePageRank

"""
This class implements the pagerank algorithm with
backwards edges as described in the second part of
the project.
"""
class BackedgesPageRank(SimplePageRank):

    """
    The implementation of __init__ and compute_pagerank should
    still be the same as SimplePageRank.
    You are free to override them if you so desire, but the signatures
    must remain the same.
    """
    def __init__(self, input_rdd):
        self.input_rdd = input_rdd

    def compute_pagerank(self, num_iters):
        nodes = self.initialize_nodes(self.input_rdd)
        num_nodes = nodes.count()
        for i in range(0, num_iters):
            nodes = self.update_weights(nodes, num_nodes)
        return self.format_output(nodes)

    """
    This time you will be responsible for implementing the initialization
    as well.
    Think about what additional information your data structure needs
    compared to the old case to compute weight transfers from pressing
    the 'back' button.
    """
    @staticmethod
    def initialize_nodes(input_rdd):
        # The pattern that this solution uses is to keep track of
        # (node, (weight, targets, old_weight)) for each iteration.
        # When calculating the score for the next iteration, you
        # know that 10% of the score you sent out from the previous
        # iteration will get sent back.

        def emit_edges(line):
            if len(line) == 0 or line[0] == "#":
                return []
            source, target = tuple(map(int, line.split()))
            edge = (source, frozenset([target]))
            self_source = (source, frozenset())
            self_target = (target, frozenset())
            return [edge, self_source, self_target]

        def reduce_edges(e1, e2):
            return e1 | e2

        def initialize_weights((source, targets)):
            return (source, (1.0, targets, 1.0))

        nodes = input_rdd\
                .flatMap(emit_edges)\
                .reduceByKey(reduce_edges)\
                .map(initialize_weights)

        return nodes

    """
    You will also implement update_weights and format_output from scratch.
    You may find the distribute and collect pattern from SimplePageRank
    to be suitable, but you are free to do whatever you want as long
    as it results in the correct output.
    """
    @staticmethod
    def update_weights(nodes, num_nodes):

        def distribute_weights((node, (weight, targets, backedge))):
            """ Set weight values for 4 cases """
            returnToSelf = 0.05 * weight
            distToEdges = 0.85 / len(targets) * weight if targets else 0.85 / (num_nodes - 1 ) * weight

            nextBackWeight = weight
            """ Add 5% of weight to current node """
            newWeights = [(node, (returnToSelf + (.1 * backedge), nextBackWeight, targets))]

            """ Iterates all nodes (when no targets) to add 85%/#nodes-1 to each """
            if targets:
                for t in targets:
                    newWeights.append((t, (distToEdges, 0, None)))
            """if not targets:"""
            if not targets:
                for n in range(0,num_nodes):
                    if n != node:
                        newWeights.append((n, (distToEdges, 0, None)))

            return newWeights

        def collect_weights((node, newWeights)):
            weight = 0
            targets = None
            backEdge = None
            for entry in newWeights:
                if entry[2] != None:
                    backEdge = entry[1]
                    targets = entry[2]
                weight += entry[0]

            return (node, (weight, targets, backEdge))

        return nodes\
                .flatMap(distribute_weights)\
                .groupByKey()\
                .map(collect_weights)

    @staticmethod
    def format_output(nodes):
        return nodes\
        .map(lambda (node, (weight, targets, old_weight)): (weight, node))\
        .sortByKey(ascending = False)\
        .map(lambda (weight, node): (node, weight))
