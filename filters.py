"""

"""

import constants

def pq_filter(pq):
    pass

def terrain_filter(dsm):
    pass

def eo_filter(source):
    """
    Find where there is no data

    Input must be dataset (since bands could have different nodata values).

    Contiguity can easily be tested either here or using PQ.
    """
    nodata_bools = source.apply(lambda array: array == array.nodata).to_array(dim='band')

    nothingness = nodata_bools.all(dim='band')
    #noncontiguous = nodata_bools.any(dim='band')

    return (constants.NO_DATA * nothingness) # | (constants.MASKED_NO_CONTIGUITY * noncontiguous)

