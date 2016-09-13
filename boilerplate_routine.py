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

        bands = ['blue','green','red','nir','swir1','swir2']

        import datacube
        dc = datacube.Datacube()


        ################################################################################################
        """
        Non-gridworkflow equivalent:

        source = dc.load(product='ls5_nbar_albers', time=time, measurements=bands, **extent).isel(time=0)
        pq = dc.load(product='ls5_pq_albers', time=time, **extent).isel(time=0)
        dsm = dc.load(product='dsm1sv10', output_crs=source.crs, resampling='cubic', resolution=(-25,25), **extent).isel(time=0)

        water = core_func(source, pq, dsm)
        """

        gw = datacube.api.GridWorkflow(dc.index, product='ls5_nbar_albers') # take GridSpec from EO archive 

        source_loadables = gw.list_tiles(product='ls5_nbar_albers', time=time, **extent)
        pq_loadables = gw.list_tiles(product='ls5_pq_albers', time=time, **extent)
        dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)
        wofls_loadables = {}


        assert len(set(t for (x,y,t) in dsm_loadables)) == 1 # assume mosaic won't require extra fusing
        timeless = lambda (x,y,t): (x,y)
        dsm_loadables = {timeless(key):val for key,val in dsm_loadables.items()} # make mosaic atemporal


        # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
        xyt = set(source_loadables) & set(pq_loadables) .difference(set(wofls_loadables))
        valid_loadables = {key: (source_loadables[key], pq_loadables[key], dsm_loadables[timeless(key)])
                           for key in xyt if timeless(key) in dsm_loadables}
        
        def package(s,p,d):
            """Wraps core function and data loading"""
            source = gw.load(s, measurements=bands).isel(time=0)
            pq = gw.load(p).isel(time=0)
            dsm = gw.load(d).isel(time=0) # resampling='cubic' : need bug fixed

            result = core_func(source, pq, dsm)

            print type(result)
            return result

        water = package(*valid_loadables.values()[1])



        ################################################################################################

        

        # visualisation
        import numpy as np
        import matplotlib.pyplot as plt
        pretty = np.empty_like(water, dtype=np.float32)
        pretty[:,:] = np.nan
        pretty[water.data != 0] = 1 # red masking
        pretty[water.data == 128] = 0 # blue water
        plt.imshow(source.red.data, cmap='gray')
        plt.imshow(pretty, alpha=0.4)
        plt.show()


    return main

