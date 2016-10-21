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

Geofabric (7 vector layers)
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


"""
Development strategy:

The total mosaic could be of order 100GB uncompressed.
A single file with chunked compression might perform well for localised reads,
but will likely exceed memory availability.

Therefore, split into tiles for creation, and consider amalgamating later.

Initial approach of operating for arbitrary region.

To start with, just plot to confirm correct reading of all 11 datasets.

"""


