"""

There are 13 ancillary inputs (from five sources) for the confidence layer,
including 2 that are unused (zero-weighted).

The algorithm permits these 11 to be synthesized into a single once-off mosaic,
for combining with the water observation frequencies:

Confidence = LogisticFunction( beta_0 * wofl_summary + ancillary_layer_mosaic )

This script prepares that mosaic, in the native resolution of wofs (albers).




WOfS is specified by the journal article:
http://dx.doi.org/10.1016/j.rse.2015.11.003
Gives their linear weights in table 4 (reproduced below as constants).
This code ignores (obviously) the two additional variables with zero weights.

Slope in decimal degrees. 
Derived from the same (1s SRTM) DSM as already used for the extents.

Multi-resolution Valley Bottom Flatness
http://doi.org/10.4225/08/5701C885AB4FE
CSIRO; CC-BY 4.0 Int; 1 arcsecond; 0.7GB; single integer band

Geofabric (7 vector layers from Surface Cartography)
https://data.gov.au/dataset/australian-hydrological-geospatial-fabric-geofabric
BOM; CC-BY 3.0 AU; v2.1.1; ESRI .gdb; 

MODIS Open Water Likelihood
CSIRO? ....???

Urban Centre and Locality (ASGS vol.4 2011)
http://abs.gov.au/AUSSTATS/abs@.nsf/Lookup/1270.0.55.004Main+Features1July%202011?OpenDocument
ABS; CC BY 2.5 AU; shapefile + csv; 30MB



"""

# Weights from WOfS table: 
wofl_wet_freq = 0.1703
MrVBF = 0.1671
MODIS_OWL = 0.0336
slope = -0.2522
geofabric_foreshore = 4.2062
geofabric_pondage = -5.4692
geofabric_reservoir = 0.6574
geofabric_flat = 0.7700
geofabric_lake = 1.9992
geofabric_swamp = 1.3231
geofabric_watercourse = 1.9206
urban_areas	 = -4.9358
# (note WOfS excludes geofabric canal and rapid, by zero weighting)


# the five ancillary sources 
MrVBF_path = ""
geofabric_path = ""
ucl_path = "/g/data/v10/wofs/ancillary/ucl/UCL_2011_AUST.shp"
owl_path = ""
dsm_productname = ""

"""
Development strategy:

The total mosaic could be of order 100GB uncompressed.
A single file with chunked compression might perform well for localised reads,
but will likely exceed memory availability.

Therefore, split into tiles for creation, and consider amalgamating later.

Initial approach of operating for arbitrary region.

To start with, just plot to confirm correct reading of all 11 datasets.

Outline for vector stuff:
    - fiona can read the shapefile
    - alternatively, geopandas can read and filter on attributes
    - may need to reproject into desired crs, e.g. by shapely
    - rasterio can rasterise vectors (remaining in their native coord sys)
      given an array shape and a transform (affine geocoding)
    - note, should consider how want partial pixel coverage treated?
    - datacube gridspec knows the crs, tile size, resolution, origin.
      can obtain via index.products.get_by_name.grid_spec


"""
import fiona

fiona.

def urban():
    """
    Only concerned with areas of 100k population or greater (--WOfS paper).
    
    UCL has type (SOS) and population (SSR) fields:
    >>> geopandas.read_file(ucl_path)[['SOS_NAME11','SSR_NAME11']].drop_duplicates()
    Output demonstrates that we only want 'Major Urban' type rows.
    """
    pass


if __name__ == '__main__':
    print 'hello'