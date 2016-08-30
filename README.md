Why is there so much code anyway in the NFRIP and WOFS repos?
And so many versions intermingled?
I reckon it is nigh unmaintainable, despite that the algorithm is simple:

Algorithm
=========

Stage One: Water tiles
----------------------

###Part i: Decision tree

6 Landsat input bands -> classifier -> boolean output band

The classifier is just a tree with 21 nodes (published).

###Part ii: Filter masks
Mask flags are accumulated onto the output band.

Input: the landsat image, the pixel quality product, and the elevation model.

The difficulty here is generating some of the flags (e.g. terrain shadow).


Stage Two: Summary statistics
-----------------------------

Inputs: 
    0. mean mosaic of the water tiles,
    1. multi-res valley bottom flatness,
    2. MODIS open water likelihood, hydrological geofabric,
    3. slope,
    4-12. hydrological geofabric (boolean vectors),
    13. Aus Stat Geog Standard (urban boolean).

Logistic function wrapping a weighted sum. Looks cheap other than the mosaic.

