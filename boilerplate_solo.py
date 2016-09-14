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


        source = dc.load(product='ls8_nbar_albers', time=time, measurements=bands, **extent)
        print len(source.time)
        print source.time.values
        pq = dc.load(product='ls8_pq_albers', time=time, **extent)
        dsm = dc.load(product='dsm1sv10', output_crs=source.crs, resampling='cubic', resolution=(-25,25), **extent).isel(time=0)

        # produce results as 3D dataset
        import xarray
        ti = pq.time
        waters = xarray.concat((core_func(source.sel(time=t), pq.sel(time=t), dsm) for t in ti.values), ti).to_dataset(name='water')

        # save output
        waters.attrs['crs'] = source.crs
        waters.water['crs'] = source.crs # datacube API may expect this attribute to also be set to something
        try:
            datacube.storage.storage.write_dataset_to_netcdf(waters,"waters.nc")
        except RuntimeError: 
            pass # does not overwrite if already exists

        # visualisation
        import numpy as np
        import matplotlib.pyplot as plt
        import math
        n = len(pq.time)
        n1 = int(math.ceil(math.sqrt(n)))
        n2 = int(math.ceil(float(n)/n1))
        fig,axes = plt.subplots(n2,n1)
        for ax,t in zip(axes.ravel() if type(axes)==np.ndarray else [axes], pq.time.values):
            water = waters.sel(time=t).water
            background = source.sel(time=t).red.data            
            pretty = np.empty_like(water, dtype=np.float32)
            pretty[:,:] = np.nan
            pretty[water.data != 0] = 1 # red masking
            pretty[water.data == 128] = 0 # blue water
            a = ax.imshow(water) # for cursor data not display
            b = ax.imshow(background, cmap='gray')
            c = ax.imshow(pretty, alpha=0.4, clim=(0,1))
            b.get_cursor_data = a.get_cursor_data # bitfield on mouseover
            c.get_cursor_data = a.get_cursor_data
            ax.set_title(t)
        plt.show()


    return main

