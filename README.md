Currently, the NFRIP and WOfS repositories have large volumes of code, which impinges on their maintainability. This code-base aims to be tidier.

Algorithm
=========
The algorithm is rather simple.


Wofls
-----

Water Observation Feature Layers; the temporal foliation of water extents. These consist of an 8-bit integer raster band.

- **Decision tree:** The standard classifier is *band maths* performed on 6 EO source bands (TM 1-5, 7). A published tree with 21 nodes, producing boolean output, comprising of thresholds applied to three raw bands (TM 1, 3, 7) and three band-pair ratio-indices (NDI 52, 43, 72).
- **Filter masks:** various flags are accumulated onto the output band. Inputs are the landsat image, the pixel quality product, and the elevation model. (The difficulty is generating some of the flags, e.g. terrain shadow.)


Summary
-------

The summary product has multiple parts: 

- a mean mosaic of the wofls (i.e. fraction of clear observations that are wet);
- a confidence estimate. This is a logistic function wrapping a linear combination (with published weights) of several inputs: 0. mean mosaic of the wofls, 1. multi-res valley bottom flatness, 2. MODIS open water likelihood, hydrological geofabric, 3. slope, 4-12. hydrological geofabric (boolean vectors), 13. Aus Stat Geog Standard (urban boolean).
- Filtered summary, i.e., mean mosaic clipped to always-dry where confidence is below a threshold. (Would also be interesting to see confidence applied as an opacity alpha channel to the mean mosaic?)


Notes and ideas
===============

Profiling
---------

Implementation of potential optimisations was deliberately deferred, until memory, CPU and IO profiling could take place.

Results (below) indicate that memory is already within the 2GB/core available, that IO is not a significant bottleneck (before scaling), and that speed is unlikely to improve dramatically (since limited by intrinsically demanding aspects of the current terrain-shadow algorithm); therefore significant optimisation effort may not be warranted. 

19/9/02016: Querying test cube and writing 4 tiles (16MB each). /usr/bin/time showed 1min30sec walltime, ~135% CPU usage, ~10% system (rather than user time), ~1.5GB max resident. cProfile time graph indicated 2.4% spent on imports, 4% on database queries, 26% on grid workflow (including >5% on rasterio read and 19% on rasterio reproject) and 65% on the core algorithm. The latter is dominated by the terrain filtering (56%, alongside 4.4% decision tree, 3.3% PQ, 1.1% EO filter). It includes 1.9% on the Sobel operation, 9.5% row shading (of which 8% is python code, as is 5.8% of shadows and slope), and 37% rotating the image (scipy). Dilation also totals 4%. Summary:

- 20% potential speedup by storing DSM in the same projection as EO, or by orchestrating execution to avoid reloading DSM redundantly.
- Most of the time is spent on terrain, but only 5-10% speedup plausible by better implementation.
- Most limiting factor is rotating the DSM (to approximately align with sunlight) but nontrivial to improve or mitigate this. (May or may not be amenable to cheaper interpolation methods or an algorithm that traverses the array differently.)


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

