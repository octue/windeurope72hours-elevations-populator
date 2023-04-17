from h3.api.basic_int import h3_get_resolution, h3_to_children, h3_to_parent


def get_descendents_down_to_maximum_resolution(cell, maximum_resolution):
    """Get all descendents of the cell down to the maximum resolution inclusively. If the resolution of the cell is
    the same as the maximum resolution, the cell is simply returned as a single-element set.

    :param int cell: the index of the cell to get the descendents of
    :param int maximum_resolution: the highest resolution (smallest cell size) to get the descendents down to inclusively
    :return set: the indexes of the descendents of the cell
    """
    descendents = set()

    if h3_get_resolution(cell) == maximum_resolution:
        return {cell}

    for child in h3_to_children(cell):
        descendents |= get_descendents_down_to_maximum_resolution(child, maximum_resolution)

    return descendents


def get_ancestors_up_to_minimum_resolution(cell, minimum_resolution):
    """Get the ancestors of the cell up to the minimum resolution inclusively. If the cell resolution is the same
    as the minimum resolution, the cell is simply returned as a single-element list.

    :param int cell: the index of the cell to get the ancestors of
    :param int minimum_resolution: the lowest resolution (largest cell size) to get the ancestors up to inclusively
    :return list: the ancestors of the cell
    """
    if h3_get_resolution(cell) == minimum_resolution:
        return [cell]

    ancestors = []

    while h3_get_resolution(cell) > minimum_resolution:
        # Add the cell's parent to the list of ancestors.
        cell = h3_to_parent(cell)
        ancestors.append(cell)

    return ancestors


def get_ancestors_up_to_minimum_resolution_as_pyramid(cells, minimum_resolution):
    """Get the ancestors of all the cells up to the minimum resolution inclusively as an inverted pyramid where each
    level of the pyramid contains ancestors of the same resolution. The zeroth level is the set of immediate parents
    and the final level is the set of minimum resolution ancestors. This format is useful when recursing down the
    resolutions (i.e. to cells of larger and larger area) to calculate the elevation of each parent based on the
    average of its children's elevations. All the given cells should be of the same resolution.

    For example, if given a list of cells of resolution 12 and the minimum resolution is 9, the pyramid looks like
    this:

        [
            {Level 11 ancestors (most)},
            {Level 10 ancestors (fewer)},
            {Level 9 ancestors (fewest)},
        ]

    :param iter(int) cells: the indexes of the cells to get the ancestors for
    :param int minimum_resolution: the lowest resolution (largest cell size) to get the ancestors up to inclusively
    :return list(set(int)): the ancestors as an inverted pyramid
    """
    pyramid = list(zip(*[get_ancestors_up_to_minimum_resolution(cell, minimum_resolution) for cell in cells]))

    # Deduplicate each level.
    for i, cells in enumerate(pyramid):
        pyramid[i] = set(cells)

    return pyramid
