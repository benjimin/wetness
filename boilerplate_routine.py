"""
This module encapsulates all the "boilerplate" necessary
to convert the WOFL algorithm to an application for automating
WOFL production (in an "operations" context).

This file uses the template of other datacube applications
(e.g. fractional cover and NDVI) and specialises for WOFLs.

This approach was pragmatic, given development constraints.
A more elegant design would have much of this migrated into 
the datacube repo, to minimise application-specific maintenance.
"""


import datacube
import pathlib
import errno 
import xarray
import pandas

bands = ['blue','green','red','nir','swir1','swir2']
platforms = ['ls8', 'ls7', 'ls5']

destination = '/short/v10/datacube/wofs'
filename_template = '{sensor}_WATER/{tile_index[0]}_{tile_index[1]}/' + \
                    '{sensor}_WATER_3577_{tile_index[0]}_{tile_index[1]}_{time}.nc'
                    # note 3577 refers to the (EPSG) projection of the GridSpec (inherited from NBAR).
sensor = {'ls8':'LS8_OLI', 'ls7':'LS7_ETM', 'ls5':'LS5_TM'}

def wofloven(time, **extent):
    """Annotator for WOFL workflow""" 
    def main(core_func):
        """Continental-scale WOFL-specific machinery"""
        print core_func.__name__

        # The WOFS storage format will be chosen to mirror the EO archive.

        # Inconveniently PQ is not stored as a measurement band within NBAR.
        # Simplest hack is to query separately and join (afterward) on time.
        # Later will explore joining beforehand (by source) and streaming results.
        # Ultimately must also exclude tiles where WOFL exists already.

        # The DSM has different resolution/CRS from the EO data,
        # so entrust the API to reconstitute into a matching format.
       
        def package(nbar, pixelquality, elevation, file_path=pathlib.Path('waters.nc'), core_func=core_func):
            """Wraps core algorithm and data IO, to be executed by worker drones without database access.
            
            Arguments:
                - three loadables representing xarray datasets of the input data
                - destination for storing output
            Returns:
                - datacube-indexable representation of the output data
            """

            if file_path.exists():
                raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))

            gw = datacube.api.GridWorkflow # shorthand

            source = gw.load(nbar, measurements=bands)
            pq = gw.load(pixelquality)
            dsm = gw.load(elevation, resampling='cubic') # resampling='cubic' : need bug fixed

            result = core_func(*(_.isel(time=0) for _ in [source, pq, dsm]))

            """import matplotlib.pyplot as plt
            plt.imshow(source.isel(time=0).red.data)
            plt.show()
            plt.imshow(result.data)
            plt.show()"""

            # Convert 2D DataArray to 3D DataSet
            result = xarray.concat([result], source.time).to_dataset(name='water')

            # write output
            result.attrs['crs'] = source.crs
            result.water.attrs['crs'] = source.crs # datacube API may expect this attribute to also be set to something
            datacube.storage.storage.write_dataset_to_netcdf(result,file_path)

            #ds = datacube.utils.make_dataset(..
            return 



        def woflingredients(index, time=time, extent=extent):
            """ Prepare task-parameters (a series of nbar,ps,dsm[,filename] loadables) for dispatch to workers.

            The concept is that workers shall not interact with the database index.
            Instead, workers must be supplied file-paths as necessary for data I/O.

            This function is the equivalent of an SQL join query,
            and is required as a workaround for datacube API abstraction layering.        
            """
            gw = datacube.api.GridWorkflow(index, product='ls5_nbar_albers') # clone GridSpec from EO archive 

            for platform in platforms:
                source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
                pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)
                dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)
                wofls_loadables = {}

                assert len(set(t for (x,y,t) in dsm_loadables)) == 1 # assume mosaic won't require extra fusing
                dsm_loadables = {(x,y):val for (x,y,t),val in dsm_loadables.items()} # make mosaic atemporal

                # only valid where EO, PQ and DSM are *all* available (and WOFL isn't yet)
                keys = set(source_loadables) & set(pq_loadables) .difference(set(wofls_loadables))
                # should sort spatially, consider repartitioning workload to minimise DSM reads.
                for x,y,t in keys:
                    if (x,y) in dsm_loadables: # filter complete
                        fn = filename_template.format(sensor=sensor[platform],
                                                      tile_index=(x,y),
                                                      time=pandas.to_datetime(t).strftime('%Y%m%d%H%M%S%f'))
                        s,p,d = map(gw.update_tile_lineage, # fully flesh-out the metadata
                                    [ source_loadables[(x,y,t)], pq_loadables[(x,y,t)], dsm_loadables[(x,y)] ])
                        yield (s,p,d, pathlib.Path(destination,fn))

        #################
        # main app logic

        dc = datacube.Datacube()

        valid_loadables = list(woflingredients(dc.index))
        print len(valid_loadables)
        valid_loadables = valid_loadables[:1] # trim for debugging.
        
        for task in valid_loadables:
            package(*task)



    return main



