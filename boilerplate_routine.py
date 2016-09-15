bands = ['blue','green','red','nir','swir1','swir2']
platforms = ['ls8', 'ls7', 'ls5']


def woflingredients(gw, platform, time, **extent):
    """ Generate dict of valid (x,y,t):(nbar,pq,dsm) loadables 

    This function is the equivalent of an SQL join query,
    and is required as a workaround for datacube API abstraction layering.        
    """
    source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
    pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)
    dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)
    wofls_loadables = {}

    assert len(set(t for (x,y,t) in dsm_loadables)) == 1 # assume mosaic won't require extra fusing
    timeless = lambda (x,y,t): (x,y)
    dsm_loadables = {timeless(key):val for key,val in dsm_loadables.items()} # make mosaic atemporal

    # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
    xyt = set(source_loadables) & set(pq_loadables) .difference(set(wofls_loadables))
    valid_loadables = {key: (source_loadables[key], pq_loadables[key], dsm_loadables[timeless(key)])
                       for key in xyt if timeless(key) in dsm_loadables}
    return valid_loadables


       
def make_tasks(index, config=None, **dummy):
    """ Prepare task-parameters which will be dispatched to workers. 

    The concept is that workers shall not interact with the database index.
    Instead, workers must be supplied file-paths as necessary for data I/O.

    The parameters for each task are supplied as a dict.
    """
    pass
        

def wofloven(time, **extent):
    """Annotator for WOFL workflow""" 
    def main(core_func):
        print core_func.__name__

        # The WOFS storage format will be chosen to mirror the EO archive.

        # Inconveniently PQ is not stored as a measurement band within NBAR.
        # Simplest hack is to query separately and join (afterward) on time.
        # Later will explore joining beforehand (by source) and streaming results.
        # Ultimately must also exclude tiles where WOFL exists already.

        # The DSM has different resolution/CRS from the EO data,
        # so entrust the API to reconstitute into a matching format.


        import datacube
        dc = datacube.Datacube()
        gw = datacube.api.GridWorkflow(dc.index, product='ls5_nbar_albers') # clone GridSpec from EO archive 

        valid_loadables = dict(set().union(*(woflingredients(gw, p, time, **extent).items() for p in platforms)))
        # should sort spatially, consider partitioning workload to minimise DSM reads.

        valid_loadables = dict(valid_loadables.items()[:2]) # trim for debugging.
        
        import pathlib, errno
        def package(nbar, pixelquality, elevation, file_path=pathlib.Path('waters.nc')):
            """Wraps core function and data IO."""

            if file_path.exists():
                raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))

            source = gw.load(nbar, measurements=bands).isel(time=0)
            pq = gw.load(pixelquality).isel(time=0)
            dsm = gw.load(elevation, resampling='cubic').isel(time=0) # resampling='cubic' : need bug fixed

            result = core_func(source, pq, dsm)

            print result
            return 

        print len(valid_loadables)

        water = package(*valid_loadables.values()[1])



    return main

