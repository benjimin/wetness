"""
This module encapsulates machinery to translate the WOFL algorithm into
an application for automating WOFL production (in an "operations" context).
"""


import datacube
import pathlib
import errno 
import xarray
import pandas
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
      type:                 # TODO: change this label..
        bits: [0,1,2,3,4,5,6,7]
        values: 
            128: clear_wet
            0: clear_dry
        description: Classification result with unclear observations masked out
""" # end of product definition

global_attributes = """
cmi_id: "WO_25_2.0.0"
cmi_nid: "5"
title: "Water Observations 25 v. 2.0.0"
summary: "Water Observations from Space (WO_25_2.0) is a gridded dataset indicating areas where surface water has been observed using the Geoscience Australia (GA) Earth observation satellite data holdings. The current product (Version 1.5) includes observations taken between 1987 and 2016 (inclusive) from the Landsat 5, 7 and 8 satellites. WO_25_2.0 covers all of mainland Australia and Tasmania but excludes off-shore Territories.

WO_25_2.0 shows water observed for every Landsat-5, Landsat-7 and Landsat-8 image across Australia (excluding External Territories) for the period of 1987 to 2016. The dataset is updated quarterly and is expected to increase in update frequency in the future so that as a satellite acquires data, it will automatically be analysed for the presence of water and added to the WO_25_2.0 product in near real time.

WO_25_2.0 delivers data in a cumulative ''summary'' that combines all water observations from the entire time series into a single gridded dataset. The dataset includes four values for each grid cell, viz: 

-  the total number of clear observations for the cell (pixel);

-  the number of times that surface water is detected for the cell;

-  the recurrence frequency of water as a percentage of the total number of clear observations, and;

-  Confidence Level.

The Confidence Level assigned to each water observation is based on a statistical analysis of factors, including topographic position, elevation and slope, other independent satellite observations of water, topographic maps of water features, and the observation frequency. The confidence layer can be used to filter uncertain observations, for instance when displaying the data. 

In the future, WO_25_2.0 will be updated as new data are added. This is potentially possible because the dataset is produced using the Australian Geoscience Data Cube, containing GA's entire Australian Landsat archive in a supercomputing environment at the National Computational Infrastructure at the Australian National University.

The Water Observations from Space product (WO_25_2.0) is a key component of the National Flood Risk Information Portal (NFRIP), being developed by Geoscience Australia (GA). The objective of Water Observations from Space is to analyse GA's historic archive of satellite imagery to derive water observations, to help understand where flooding may have occurred in the past.

WO_25_2.0 is being developed in parallel with the National Flood Studies Database system which will provide Flood Study documentation and reports to a wide range of users. Both systems will be delivered via the internet through the NFRIP portal.

Satellite imagery has been used to map floods around the world for several years. Organisations such as the Colorado Flood Observatory in the USA and several state-based agencies in Australia regularly provide satellite-based flood extents for major flood events. GA developed a flood mapping methodology in 2008-2009 that was extensively used for the major Australian flood events since 2010, providing emergency service agencies with regional flood extent information.

The Phase 1 outputs from the NFRIP were delivered to the public in November 2012, including a proof of concept of WO_25_2.0. This displayed surface water extents for three study areas, including the original derived extents and the cumulative summary product. Subsequent stakeholder feedback has shown that the most desirable information is the summary product, providing an understanding of the long term dynamics of surface water.

The Phase 2 outputs from the NFRIP (released in April 2014), include this WO_25_2.0 product, which is now accessible to the public as web services, online viewers and the underlying data.

Confidence Level

Reviews of prototype products identified the need to indicate the level of confidence for the surface water observations. The confidence level will help the user to distinguish between unusual but valid water detections (such as flood plains which might only be observed as water once in the 15 year interval) and 'false positives' which can be caused, for instance, by steep shady slopes.
The confidence level was determined through a multiple logistic regression of water observations against several factors that would either support or contra-indicate the finding of water being present at the site.

The factors comprise:

MrVBF, a multi-resolution valley bottom flatness product (Gallant et al., 2012) derived from the Shuttle Radar Topography Mission (SRTM) as part of the Terrestrial Ecosystems Research Network. Surface water pixels identified in valley bottoms were more likely to be positively detected.

Slope calculated from SRTM Digital Surface Models. Water pixels on a slope were considered less plausible than those on a flat surface.

MODIS Open Water Likelihood (OWL) (Ticehurst et al, 2010) provides a plausibility based an independent water detection algorithm employing the MODIS sensor. If both detection algorithms agree on the presence of a surface water pixel, there is a greater plausibility that the detection is correct.

Australian Hydrological Geospatial Fabric (Geofabric) is a GIS of hydrological features derived from manually interpreted topographic map grids. If known hydrologic features (pixels) from GeoFabric coincide with detected water pixels, the plausibility of detection is greater.

P, the number of observations of water as a fraction of the number of clear observations of the target pixel. P is high for more permanent water bodies.

Built-Up areas indicating areas of dense urban development. In such areas the water detection algorithm struggles to cope with the deep shadows cast by multi-story buildings and the generally noisy spectral response created by structures. The Built-Up layer is derived from the Australian Bureau of Statistics ASGS 2011 dataset, for urban centres of populations of 100 000 and over.
"
source: "SR-N_25_2.0"
institution: "Commonwealth of Australia (Geoscience Australia)"
keywords: ""
keywords_vocabulary: ""
product_version: "2.0.0"
license: "CC BY Attribution 4.0 International License"
coverage_content_type: ""
cdm_data_type: ""
product_suite: ""
references: "Geoscience Australia (2013) Australian Reflectance Grid (ARG25) Product Information - Beta Release- External Document TRIM Ref D2013-41317, Geoscience Australia, Canberra.

Gallant, J., Dowling, T., and Austin, J. (2012): Multi-resolution Valley Bottom Flatness (MrVBF, 3&quot; resolution). v2. CSIRO. Data Collection. 10.4225/08/512EF27AC3888 http://dx.doi.org/10.4225/08/512EF27AC3888.

Ticehurst, C J., Bartsch, A., Doubkova M.,and van Dijk, A.I.J.M. (2010) Comparison of ENVISAT ASAR ASAR GM, AMSR-E Passive Microwave, and MODIS Optical Remote Sensing for Flood Monitoring in Australia. Proceedings of the 'Earth Observation and Water Cycle Science', Frascati, Italy, 18-20 November 2009 (ESA SP-674, January 2010).

N. Mueller, A. Lewis, D. Roberts, S. Ring, R. Melrose, J. Sixsmith, L. Lymburner, A. McIntyre, P. Tan, S. Curnow, A. Ip, Water observations from space: Mapping surface water from 25 years of Landsat imagery across Australia, Remote Sensing of Environment, Volume 174, 1 March 2016, Pages 341-352, ISSN 0034-4257, http://dx.doi.org/10.1016/j.rse.2015.11.003. (http://www.sciencedirect.com/science/article/pii/S0034425715301929)"
""" # end of global attributes



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
    
def box_and_envelope(loadables):
    """Utility to prepare spatial metadata"""
    # Tile loadables contain a "sources" DataArray, that is, a time series 
    # (in this case with unit length) of tuples (lest fusing may be necessary)
    # of datacube Datasets, which should each have memoised a file path
    # (extracted from the database) as well as an array extent and a valid 
    # data extent. (Note both are just named "extent" inconsistently.)
    # The latter exists as an optimisation to sometimes avoid loading large 
    # volumes of (exclusively) nodata values. 
    #assert len(set(x.geobox.extent for x in loadables)) == 1 # identical geoboxes are unequal?
    bounding_box = loadables[0].geobox.extent # inherit array-boundary from post-load data
    def valid_data_envelope(loadables=list(loadables), crs=bounding_box.crs):
        def data_outline(tile):
            parts = (ds.extent.to_crs(crs).points for ds in tile.sources.values[0])
            return datacube.utils.union_points(*parts)
        footprints = [bounding_box.points] + map(data_outline, loadables)
        overlap = reduce(datacube.utils.intersect_points, footprints)
        return datacube.model.GeoPolygon(overlap, crs)    
    return bounding_box, valid_data_envelope()

def docvariable(agdc_dataset, time):
    """Utility to convert datacube dataset to xarray/NetCDF variable"""
    array = xarray.DataArray([agdc_dataset], coords={'time':time})
    docarray = datacube.model.utils.datasets_to_doc(array)
    docarray.attrs['units'] = '1' # unitless (convention)
    return docarray




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
            for task in self.generate_tasks(index, time=t):
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


class debug_application(datacube_application):
    def main(self, index, max_tasks=2):
        """Simplified main interface, for one-run debugging"""
        tasks = list(self.generate_tasks(index, self.default_time_range, self.default_spatial_extent)) # find work to do
        print len(tasks), "tasks (total)"
        tasks = tasks[:max_tasks] # trim for debugging.     
        for task in tasks:
            print ".",
            ds = self.perform_task(*task) # do work
            index.datasets.add(ds) # index completed work
        print "Done"



class wofloven(datacube_application):
    """Specialisations for Water Observation product"""
    product_definition = definition
    info = info
    global_attributes = yaml.load(global_attributes)
    def generate_tasks(self, index, time, extent={}):
        """ Yield loadables (nbar,ps,dsm) and targets, for dispatch to workers.

        This function is the equivalent of an SQL join query,
        and is required as a workaround for datacube API abstraction layering.        
        """
        gw = datacube.api.GridWorkflow(index, product=self.product.name) # GridSpec from product definition

        wofls_loadables = gw.list_tiles(product=self.product.name, time=time, **extent)

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

    def perform_task(self, loadables, file_path):
        """ Load data, run WOFS algorithm, attach metadata, and write output.
        
        Input: 
            - three-tuple of Tile objects (NBAR, PQ, DSM)
            - path object (output file destination)
        Output:
            - indexable object (referencing output data location)
        """        
        if file_path.exists():
            raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))
            
        # load data
        protosource, protopq, protodsm = loadables
        load = datacube.api.GridWorkflow.load
        source = load(protosource, measurements=bands)
        pq = load(protopq)
        dsm = load(protodsm, resampling='cubic')
        
        # Core computation
        result = self.core(*(x.isel(time=0) for x in [source, pq, dsm]))
        
        # Convert 2D DataArray to 3D DataSet
        result = xarray.concat([result], source.time).to_dataset(name='water')
        
        # add metadata
        result.water.attrs['nodata'] = 1 # lest it default to zero (i.e. clear dry)
        result.water.attrs['units'] = '1' # unitless (convention)

        # Attach CRS. Note this is poorly represented in NetCDF-CF
        # (and unrecognised in xarray), likely improved by datacube-API model.
        result.attrs['crs'] = source.crs
        
        # inherit spatial metadata
        box, envelope = box_and_envelope(loadables)

        # Provenance tracking
        allsources = [ds for tile in loadables for ds in tile.sources.values[0]]

        # Create indexable record
        new_record = datacube.model.utils.make_dataset(
                            product=self.product,
                            sources=allsources,
                            center_time=result.time.values[0],
                            uri=file_path.absolute().as_uri(),
                            extent=box,
                            valid_data=envelope,
                            app_info=self.info )   
                            
        # inherit optional metadata from EO, for future convenience only
        def harvest(what, datasets=[ds for time in protosource.sources.values for ds in time]):
            values = [ds.metadata_doc[what] for ds in datasets]
            assert all(value==values[0] for value in values)
            return values[0]
        new_record.metadata_doc['platform'] = harvest('platform') 
        new_record.metadata_doc['instrument'] = harvest('instrument') 
        
        # copy metadata record into xarray 
        result['dataset'] = docvariable(new_record, result.time)

        # write output
        datacube.storage.storage.write_dataset_to_netcdf(
            result, file_path, global_attributes=self.global_attributes)

        return new_record
        

        