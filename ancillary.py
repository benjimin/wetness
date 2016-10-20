"""

There are 11 ancillary inputs (from five sources) used by the confidence layer.

These can be synthesized into a single once-off mosaic, for combining with
the water observation frequencies.

This script prepares that mosaic.




WOfS is specified by the journal article:
http://dx.doi.org/10.1016/j.rse.2015.11.003
Gives their linear weights.

Slope in decimal degrees. 
Derived from the same (1s SRTM) DSM as already used for the extents.

Multi-resolution Valley Bottom Flatness
http://doi.org/10.4225/08/5701C885AB4FE
CSIRO; CC-BY 4.0 Int; 1 arcsecond; 0.7GB; single integer band

Geofabric (9 vector layers)
https://data.gov.au/dataset/australian-hydrological-geospatial-fabric-geofabric
BOM; CC-BY 3.0 Aus; v2.1.1; ESRI .gdb; 

MODIS Open Water Likelihood


ABS ASGS 2011 Urban Centre and Locality




"""

