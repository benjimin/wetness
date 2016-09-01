"""
Get data. Run classifier.
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
dsm = dc.load(product='dsm1sv10', **extent).isel(time=0)#.elevation

#---------------------------------------------------WOFS



# apply decision tree

import classifier_josh as classifier
water = classifier.classify(source.to_array(dim='band').data)

# apply filters

import filters
water = water | filters.eo_filter(source) \
              | filters.pq_filter(pq.data) | filters.terrain_filter(dsm, source)

#TODO: unset wetness if not clear? i.e. where > 128, or/minus..

#-------------------------------------------------------

# Visualise result

pretty = np.empty_like(water, dtype=np.float32)
pretty[:,:] = np.nan
pretty[water.data != 0] = 1
pretty[water.data == 128] = 0


import matplotlib.pyplot as plt
for layer in [pretty]:#[source.red.data, water.data, pretty]:
    print np.min(layer), np.max(layer), np.mean(layer)
    plt.imshow(layer)
    plt.colorbar()
    plt.show()

