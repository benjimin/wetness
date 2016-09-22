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
import numpy as np
import datacube.model.utils
import yaml

info = {'lineage': { 'algorithm': { 'name': "WOFS decision-tree water extents",
                                    'version': 'unknown',
                                    'repo_url': 'https://github.com/benjimin/wetness' }}}

bands = ['blue','green','red','nir','swir1','swir2'] # inputs needed from EO data

sensor = {'ls8':'LS8_OLI', 'ls7':'LS7_ETM', 'ls5':'LS5_TM'} # { nbar-prefix : filename-prefix } for platforms

destination = '/short/v10/datacube/wofs'
filename_template = '{sensor}_WATER/{tile_index[0]}_{tile_index[1]}/' + \
                    '{sensor}_WATER_3577_{tile_index[0]}_{tile_index[1]}_{time}.nc'
                    # note 3577 (hard-coded) refers to the (EPSG) projection of the GridSpec (inherited from NBAR).

definition = """
name: wofs_albers
description: Historic Flood Mapping Water Observations from Space
managed: True
metadata_type: eo
metadata:               # issue: eo may expect a single platform/sensor declaration
  product_type: wofs
  format: 
    name: NetCDF
storage:                # this section should be inherited from nbar, or otherwise should determine the gridworkflow/gridspec
    crs: EPSG:3577
    resolution:
        x: 25
        y: -25
    tile_size:
        x: 100000.0
        y: 100000.0
    driver: NetCDF CF
    dimension_order: [time, y, x]
    chunking:
        x: 200
        y: 200
        time: 1   
measurements:
  - name: water
    dtype: uint8
    flags_definition:
      - name: nodata
        bits: 0                
        description: Missing all necessary earth-observation bands
      - name: noncontiguous
        bits: 1
        description: Missing some necessary earth-observation bands
      - name: sea
        bits: 2
        description: Marine open-water rather than terrestrial area
      - name: terrain_shadow
        bits: 3
        description: Terrain shadow or low solar angle
      - name: high_slope
        bits: 4
        description: Steep terrain
      - name: cloud_shadow
        bits: 5
        description: Cloud shadow
      - name: cloud
        bits: 6
        description: Obscured by cloud
      - name: wet
        bits: 7
        description: Raw classification before masking
      - name: clear
        bits: [0,1,2,3,4,5,6]
        values: 
            0: True
        description: Clear observation
      - name: clear_water
        bits: [0,1,2,3,4,5,6,7]
        values: 
            128: True
        description: Clear observation of water
      - name: clear_dry
        bits: [0,1,2,3,4,5,6,7]
        values: 
            0: True
        description: Clear observation of absence of water
      - name: final
        bits: [0,1,2,3,4,5,6,7]
        values: 
            128: water
            0: dry
        description: Classification result with unclear observations masked out
""" # end of product definition


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

        # Note, dealing with each temporal layer individually.
        # (Because of the elevation mosaic, this may be inefficient.)

        def package(loadables, file_path=pathlib.Path('waters.nc'), core_func=core_func, info=info):
            """Wraps core algorithm and data IO, to be executed by worker drones without database access.
            
            Arguments:
                - three loadables (Tile objects containing lists of datafile paths) 
                  from which to generate input xarray datasets
                - destination for storing output
            Returns:
                - datacube-indexable representation of the output data
            """

            if file_path.exists():
                raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))

            gw = datacube.api.GridWorkflow # shorthand

            # Load data
            source, pq, dsm = loadables

            source = gw.load(source, measurements=bands)
            pq = gw.load(pq)
            dsm = gw.load(dsm, resampling='cubic')

            # Core computation
            result = core_func(*(_.isel(time=0) for _ in [source, pq, dsm]))

            # Convert 2D DataArray to 3D DataSet
            result = xarray.concat([result], source.time).to_dataset(name='water')

            # Prepare spatial metadata

            # Tile loadables contain a "sources" DataArray, that is, a time series 
            # (in this case with unit length) of tuples (lest fusing may be necessary)
            # of datacube Datasets, which should each have memoised a file path
            # (extracted from the database) as well as an array extent and a valid 
            # data extent. (Note both are just named "extent" inconsistently.)
            # The latter exists as an optimisation to sometimes avoid loading large 
            # volumes of (exclusively) nodata values. 

            bounding_box = source.geobox.extent # inherit array-boundary from post-load data

            def valid_data_envelope(loadables=list(loadables), crs=bounding_box.crs):
                def data_outline(tile):
                    parts = (ds.extent.to_crs(crs).points for ds in tile.sources.values[0])
                    return datacube.utils.union_points(*parts)
                footprints = [bounding_box.points] + map(data_outline, loadables)
                overlap = reduce(datacube.utils.intersect_points, footprints)
                return datacube.model.GeoPolygon(overlap, crs)

            # Provenance tracking
            allsources = [ds for tile in loadables for ds in tile.sources.values[0]]

            # Attach metadata (as an xarray/datavariable of datacube.dataset yamls)
            new_record = datacube.model.utils.make_dataset(
                                product=product,
                                sources=allsources,
                                center_time=result.time.values[0],
                                uri=file_path.absolute().as_uri(),
                                extent=bounding_box,
                                valid_data=valid_data_envelope(),
                                app_info=info )
            def xarrayify(item, t=result.time):
                return xarray.DataArray.from_series( pandas.Series([item], t.to_series()) )
            docarray = datacube.model.utils.datasets_to_doc(xarrayify(new_record))
            #result['dataset'] = docarray

            # Attach CRS. Note this is poorly represented in NetCDF-CF
            # (and unrecognised in xarray), likely improved by datacube-API model.
            result.attrs['crs'] = source.crs

            # write output
            datacube.storage.storage.write_dataset_to_netcdf(result,file_path)

            return new_record



        def woflingredients(index, time=time, extent=extent):
            """ Prepare task-parameters (a series of nbar,ps,dsm[,filename] loadables) for dispatch to workers.

            The concept is that workers shall not interact with the database index.
            Instead, workers must be supplied file-paths as necessary for data I/O.

            This function is the equivalent of an SQL join query,
            and is required as a workaround for datacube API abstraction layering.        
            """
            gw = datacube.api.GridWorkflow(index, product='ls5_nbar_albers') # clone GridSpec from EO archive 

            for platform in sensor.keys():
                source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
                pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)
                dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)
                wofls_loadables = gw.list_tiles(product=product.name, time=time, **extent)

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
                        yield ((s,p,d), pathlib.Path(destination,fn))


        #################
        # main app logic


        dc = datacube.Datacube()

        # convert product definition document to DatasetType object
        _definition = yaml.load(definition)
        metadata_type = dc.index.metadata_types.get_by_name(_definition['metadata_type'])
        product = datacube.model.DatasetType(metadata_type, _definition)     
        
        print product.name
        valid_loadables = list(woflingredients(dc.index))
        print len(valid_loadables)
        valid_loadables = valid_loadables[:2] # trim for debugging.
        

        for task in valid_loadables:
            ds = package(*task)
            print type(ds)
            


    return main



