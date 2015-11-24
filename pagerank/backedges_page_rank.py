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



        return input_rdd.filter(lambda x: False)

    """
    You will also implement update_weights and format_output from scratch.
    You may find the distribute and collect pattern from SimplePageRank
    to be suitable, but you are free to do whatever you want as long
    as it results in the correct output.
    """
    @staticmethod
    def update_weights(nodes, num_nodes):

        def distribute_weights((node, (weight, targets))):
            """ Set weight values for 4 cases """
            returnToSelf = 0.05 * weight
            distToEdges = 0.85 / len(targets) * weight if targets else 0.85 / (num_nodes - 1 ) * weight
            distToAll = 0.1

            """ Add 5% of weight to current node """
            newWeights = [(node, returnToSelf)]

            """ Iterate targets to add 85%/#targets to each """
            for t in targets:
                newWeights.append((t, distToEdges))

            """ Iterates all nodes (when no targets) to add 85%/#nodes-1 to each """
            if not targets:
                for n in range(0,num_nodes):
                    if n != node:
                        newWeights.append((n, distToEdges))

            targets = (node, targets)
            newWeights.append(targets)  # Append targets for next iteration

            return newWeights

        def collect_weights((node, newWeights)):
            weight = 0.1
            targets = []
            for entry in newWeights:
                if isinstance(entry, float):
                    weight += entry
                else:
                    targets = entry

            return (node, (weight, targets))

        return nodes\
                .flatMap(distribute_weights)\
                .groupByKey()\
                .map(collect_weights)
        return nodes.filter(lambda x: False)

    @staticmethod
    def format_output(nodes):
        # YOUR CODE HERE
        return nodes.filter(lambda x: False)
