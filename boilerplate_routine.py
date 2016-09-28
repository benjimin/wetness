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

import click
import pickle
import itertools

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
metadata:
  product_type: wofs
  format: 
    name: NetCDF
storage:                # redundancy/unmaintainability: this section should be
    crs: EPSG:3577      # inherited from nbar, or alternatively should determine the gridworkflow/gridspec
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
    nodata: 1
    units: '1'
    flags_definition:
      type:
        bits: [0,1,2,3,4,5,6,7]
        values: 
            128: clear_wet
            0: clear_dry
        description: Classification result with unclear observations masked out
""" # end of product definition


def unpickle_stream(pickle_file):
    """Utility to stream unpickled objects from a file"""
    s = pickle.Unpickler(pickle_file)
    while True:
        try:
            yield s.load()
        except EOFError:
            raise StopIteration
            
def map_orderless(core,tasks,queue=50):
    """Utility to stream tasks through compute resources"""
    import distributed # slow import
    ex = distributed.Client() # executor  
    
    tasks = (i for i in tasks) # ensure input is a generator
      
    # pre-fill queue
    results = [ex.submit(core,*t) for t in itertools.islice(tasks, queue)]
           
    while results:
        result = next(distributed.as_completed(results)) # block
        results.remove(result)                  # pop completed

        task = next(tasks, None)
        if task is not None:
            results.append(ex.submit(core,*task)) # queue another
        
        yield result.result() # unwrap future

def get_product(index, definition):
    """Utility to get database-record corresponding to product-definition"""
    parsed = yaml.load(definition)
    metadata_type = index.metadata_types.get_by_name(parsed['metadata_type'])
    prototype = datacube.model.DatasetType(metadata_type, parsed)
    return index.products.add(prototype) # idempotent
    
    


class datacube_application:
    """Nonspecific application workflow."""
    product_definition = NotImplemented
    def generate_tasks(self, index, time_range):
        """Prepare stream of tasks (i.e. of argument tuples)."""
        raise NotImplemented
    def perform_task(self, *args):
        """Execute computation without database interaction"""
        raise NotImplemented
    def __init__(self, time, **extent):
        """Collect keyword options"""
        self.default_time_range = time
        self.default_spatial_extent = extent
    def __call__(self, algorithm):
        """Annotator API for application
        
        >>> @datacube_application(**options)
        >>> def myfunction(input_chunk):
        >>>     return output_chunk
        """
        index = datacube.Datacube().index        
        self.core = algorithm
        self.product = get_product(index, self.product_definition)
        self.main(index)
        raise SystemExit
    def main(self, index):
        """Compatibility command-line-interface"""
        
        @click.group(name=self.core.__name__)
        def cli():
            pass
        
        @cli.command(help="Pre-query tiles for one calendar year.")
        @click.argument('year', type=click.INT)
        @click.argument('taskfile', type=click.File('w'))
        @click.option('--max', default=0, help="Limit number of tasks")
        def prepare(year, taskfile, max):
            t = str(year)+'-01-01', str(year+1)+'-01-01'
            print "Querying", t[0], "to", t[1]
            stream = pickle.Pickler(taskfile)
            i = 0
            for task in self.generate_tasks(index, time_range=t):
                stream.dump(task)
                i += 1
                if i==max:
                    break
            print i, "tasks prepared"         
           
        @cli.command(help="Read pre-queried tiles and distribute computation.")
        @click.option('--backlog', default=50, help="Maximum queue length")
        @click.argument('taskfile', type=click.File('r'))
        def orchestrate(backlog, taskfile):
            tasks = unpickle_stream(taskfile)
            done_tasks = map_orderless(self.perform_task, tasks, queue=backlog)
            for i,ds in enumerate(done_tasks):
                print i
                index.datasets.add(ds, skip_sources=True) # index completed work
            print "Done"
            
        cli()


class monkeypatch_application(datacube_application)
    def main(self, index, max_tasks=2):
        """Simplified main interface, for debugging"""
        tasks = list(self.generate_tasks(index, self.default_time_range, self.default_spatial_extent)) # find work to do
        print len(tasks), "tasks (total)"
        tasks = tasks[:max_tasks] # trim for debugging.     
        for task in tasks:
            print ".",
            ds = taskdoer(*task) # do work
            index.datasets.add(ds) # index completed work
        print "Done"



class wofloven(datacube_application):
    """Specialisations for Water Observation product"""
    def generate_tasks(self, index, time, extent={}):
        """ Yield loadables (nbar,ps,dsm) and targets, for dispatch to workers.

        This function is the equivalent of an SQL join query,
        and is required as a workaround for datacube API abstraction layering.        
        """
        gw = datacube.api.GridWorkflow(index, product='ls5_nbar_albers') # clone GridSpec from EO archive 

        wofls_loadables = gw.list_tiles(product=product.name, time=time, **extent)

        for platform in sensor.keys():
            source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
            pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)
            dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)                

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



    def perform_task(self, *args):
        raise NotImplemented    




def wofloven(time, **extent):
    """Annotator for WOFL workflow""" 
    def main(core_func):time=time, extent=extent):
        """Continental-scale WOFL-specific machinery"""

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
            
            # prepare to harvest some metadata from the EO input
            def harvest(what, datasets=[ds for time in source.sources.values for ds in time]):
                values = [ds.metadata_doc[what] for ds in datasets]
                assert all(value==values[0] for value in values)
                return values[0]
            
            # load the data from disk
            source = gw.load(source, measurements=bands)
            pq = gw.load(pq)
            dsm = gw.load(dsm, resampling='cubic')

            # Core computation
            result = core_func(*(_.isel(time=0) for _ in [source, pq, dsm]))

            # Convert 2D DataArray to 3D DataSet
            result = xarray.concat([result], source.time).to_dataset(name='water')
            result.water.attrs['nodata'] = 1 # lest it default to zero (i.e. clear dry)
            result.water.attrs['units'] = '1' # unitless convention
            

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
            new_record.metadata_doc['platform'] = harvest('platform') # optional,
            new_record.metadata_doc['instrument'] = harvest('instrument') # for future convenience only
            def xarrayify(item, t=result.time):
                return xarray.DataArray([item],coords={'time':t})
            docarray = datacube.model.utils.datasets_to_doc(xarrayify(new_record))
            docarray.attrs['units'] = '1' # datavariable holding metadata must still comply with convention
            result['dataset'] = docarray

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

            wofls_loadables = gw.list_tiles(product=product.name, time=time, **extent)

            for platform in sensor.keys():
                source_loadables = gw.list_tiles(product=platform+'_nbar_albers', time=time, **extent)
                pq_loadables = gw.list_tiles(product=platform+'_pq_albers', time=time, **extent)
                dsm_loadables = gw.list_tiles(product='dsm1sv10', **extent)                

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
        product = dc.index.products.add(product) # idempotently ensure database knows this product
                                                 # and return version updated with database keys

        simplistic_app(dc.index, woflingredients, package) 
        #andrew_app(dc.index, woflingredients, package)
        #another_app(dc.index, woflingredients, package, core_func.__name__)

    return main



def another_app(index, taskmaker, taskdoer, name="application"):
    """ Compatibility interface """
    
    import click
    import pickle
    import itertools
    def unpickle_stream(f):
        s = pickle.Unpickler(f)
        while True:
            try:
                yield s.load()
            except EOFError:
                raise StopIteration                
    def map_orderless(core,tasks,queue=50):
        tasks = (i for i in tasks) # ensure input is a generator
        import distributed # slow
        ex = distributed.Client() # executor        
        # pre-fill queue
        results = [ex.submit(core,*t) for t in itertools.islice(tasks, queue)]
               
        while results:
            result = next(distributed.as_completed(results)) # block
            results.remove(result)                  # pop completed
    
            task = next(tasks, None)
            if task is not None:
                results.append(ex.submit(core,*task)) # queue another
            
            yield result.result() # unwrap future
    
    @click.group(name=name)
    def cli():
        pass
    
    @cli.command(help="Pre-query tiles for one calendar year.")
    @click.argument('year', type=click.INT)
    @click.argument('taskfile', type=click.File('w'))
    @click.option('--max', default=0, help="Limit number of tasks")
    def prepare(year, taskfile, max):
        t = str(year)+'-01-01', str(year+1)+'-01-01'
        print "Querying", t[0], "to", t[1]
        stream = pickle.Pickler(taskfile)
        i = 0
        for task in taskmaker(index, time=t):
            stream.dump(task)
            i += 1
            if i==max:
                break
        print i, "tasks prepared"
        
       
    @cli.command(help="Read pre-queried tiles and distribute computation.")
    @click.option('--backlog', default=10, help="Maximum queue length")
    @click.argument('taskfile', type=click.File('r'))
    def orchestrate(backlog, taskfile):
        tasks = unpickle_stream(taskfile)
        for i,ds in enumerate(map_orderless(taskdoer, tasks, queue=backlog)):
            print i
            index.datasets.add(ds, skip_sources=True) # index completed work
        print "Done"
        
    cli()
    
