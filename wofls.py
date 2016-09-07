"""
Produce water observation feature layers.

These are the WOfS product with time extent.
Consists of wet/dry estimates and filtering flags, 
with one-to-one correspondence to earth observation layers.
(The other wofs product is the summaries, derived from the condensed wofl mosaic.)

Issues:
    - previous documentation may be ambiguous or previous implementations may differ
      (e.g. saturation, bitfield)
    - Tile edge artifacts concerning cloud buffers and cloud or terrain shadows.
    - DSM may have different natural resolution to EO source.
      Should think about what CRS to compute in, and what resampling methods to use.
      Also, should quantify whether earth's curvature is significant on tile scale.
    - Yet to profile memory, CPU or IO usage.
"""

import numpy as np



import datacube
dc = datacube.Datacube()

# Load input data

extent = {'lon':(149.0,149.5), 'lat':(-35.0, -35.5)}
time = ('1994-09-21','1994-09-22')

bands = ['blue','green','red','nir','swir1','swir2']

source = dc.load(product='ls5_nbar_albers', measurements=bands, time=time, **extent).isel(time=0)
pq = dc.load(product='ls5_pq_albers', time=time, **extent).isel(time=0).pixelquality
dsm = dc.load(product='dsm1sv10', output_crs=source.crs, resolution=(-25,25), **extent).isel(time=0)



#---------------------------------------------------WOFLS

# apply decision tree and apply filters

import classifier_josh as classifier
import filters

water = classifier.classify(source.to_array(dim='band').data) \
        | filters.eo_filter(source) \
        | filters.pq_filter(pq.data) \
        | filters.terrain_filter(dsm, source)

assert water.dtype == np.uint8

#-------------------------------------------------------

# Visualise result

pretty = np.empty_like(water, dtype=np.float32)
pretty[:,:] = np.nan # hide dry
pretty[water.data != 0] = 1 # red masking
pretty[water.data == 128] = 0 # blue water

import matplotlib.pyplot as plt
plt.imshow(source.red.data, cmap='gray')
plt.imshow(pretty, alpha=0.5)
plt.show()


