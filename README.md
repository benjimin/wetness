Currently, the NFRIP and WOfS repositories have large volumes of code, which impinges on their maintainability.

Algorithm
---------
The algorithm is actually quite simple:


### Stage One: Wofls

Water Observation Feature Layers. These consist of a single 8-bit integer band.

#### Decision tree

The standard classifier is simply band maths performed on the 6 EO source bands. 
(A published tree with 21 nodes, where thresholds are applied to three raw bands and three band ratio indices, producing boolean output.)

#### Filter masks
Mask flags are accumulated onto the output band.

Input: the landsat image, the pixel quality product, and the elevation model.

The difficulty here is generating some of the flags (e.g. terrain shadow).


### Stage Two: Summary

Inputs:  
0. mean mosaic of the wofls,  
1. multi-res valley bottom flatness,  
2. MODIS open water likelihood, hydrological geofabric,  
3. slope,  
4-12. hydrological geofabric (boolean vectors),  
13. Aus Stat Geog Standard (urban boolean).

Logistic function wrapping a linear combination.

