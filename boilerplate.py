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

        source = dc.load(product='ls5_nbar_albers', time=time, measurements=bands, **extent).isel(time=0)
        pq = dc.load(product='ls5_pq_albers', time=time, **extent).isel(time=0)
        dsm = dc.load(product='dsm1sv10', output_crs=source.crs, resolution=(-25,25), **extent).isel(time=0)

        water = core_func(source, pq, dsm)

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

