Currently, the NFRIP and WOfS repositories have large volumes of code, which impinges on their maintainability. This code-base aims to be tidier.

Algorithm
=========
The algorithm is rather simple.


Wofls
-----

Water Observation Feature Layers. These consist of a single 8-bit integer band.

- **Decision tree:** The standard classifier is *band maths* performed on 6 EO source bands (TM 1-5, 7). A published tree with 21 nodes, producing boolean output, comprising of thresholds applied to three raw bands (TM 1, 3, 7) and three band-pair ratio-indices (NDI 52, 43, 72).
- **Filter masks:** various flags are accumulated onto the output band. Inputs are the landsat image, the pixel quality product, and the elevation model. (The difficulty is generating some of the flags, e.g. terrain shadow.)


Summary
-------

The summary product has two parts: a mean mosaic of the wofls, and a confidence estimate. The latter is a logistic function wrapping a linear combination (with published weights) of several inputs:

0. mean mosaic of the wofls,  
1. multi-res valley bottom flatness,  
2. MODIS open water likelihood, hydrological geofabric,  
3. slope,  
4-12. hydrological geofabric (boolean vectors),  
13. Aus Stat Geog Standard (urban boolean).


Notes and ideas
===============

Profiling
---------

Memory, CPU and IO profiling has yet to take place; implementation of potential optimisations has been deliberately deferred.

Classifier
----------

It may improve performance and readability to represent the decision tree as a numexpr statement (nested across multiple lines). This could additionally include some of the mask logic.

Ideally the PQ product might be a band in the EO product (and include terrain related bitflags). 

Alternative algorithms are under development elsewhere.


Terrain
-------

Terrain algorithms usually begin with finding the gradient component along each of the two axes, typically by operating with a 3x3 kernel. One example is the Rook's case (simply using nearest neighbours on either side of the pixel, which turns out to be a 2nd order finite difference method). Another is the Sobel operator, which additionally applies smoothing along the orthogonal axis. Tang and Pilesjo 2011 showed these belong to a variety of methods which produce statistically similar results (different from a more naive and unbalanced method of differencing the central cell with one neighbour along each axis). Jones 1998 found the Rook's case to give the best accuracy (narrowly followed by Sobel), but the methodology (e.g. noise-free synthetic) may have been biased (to favour balanced methods with more compact footprints). Zhou and Liu 2004 added noise to a synthetic, confirming the Rook's case to be optimal in absence of noise but the Sobel operator was more robust to the noise. 


Clouds
------

Currently, cloud and cloud shadow are detected per scene, which is suboptimal at contiguous boundaries.

Improved masking algorithms are anticipated, e.g. as median mosaics become available, or possibly incorporating weather data.

