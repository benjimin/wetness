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
source = dc.load(product='ls5_nbar_albers', measurements=bands, time=time, **extent).isel(time=0).to_array(dim='band')
pq = dc.load(product='ls5_pq_albers', time=time, **extent).isel(time=0).pixelquality
dsm = dc.load(product='dsm1sv10', **extent).isel(time=0).elevation

#---------------------------------------------------WOFS



# apply decision tree

import classifier_josh as classifier
water = classifier.classify(source.data)

# apply filters

import filters
water = filters.filter_by_PQ(water, pq)
water = filters.filter_by_DSM(water, dsm)

#-------------------------------------------------------

# Visualise result

import matplotlib.pyplot as plt
for layer in [np.squeeze(source[3,:,:].data), water]:
    print np.min(layer), np.max(layer), np.mean(layer)
    plt.imshow(layer)
    plt.colorbar()
    plt.show()

