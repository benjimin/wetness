"""
WOfS product specification:

1<<0 nodata (missing all earth observation bands)
1<<1 noncontiguous (missing any EO bands) or oversaturated/undersaturated 
1<<2 sea
1<<3 terrain shadow or low solar angle
1<<4 high slope
1<<5 cloud shadow
1<<6 cloud
1<<7 wet*

*Restriction: water bit may only be set if all other flags false.

Implication: clear dry == 0, clear wet == 128, 129-255 disallowed.

Ambiguous: are all values 0-128 valid, or are there only 8 valid
values (and if so what is their precedence)?

Question: can we start using this as a bit field rather than an enumeration?
"""




# Constants used in the Flood History Project
#
# @Author: Steven Ring, May 2013
#-------------------------------------------------------------

WATER_CONSTANTS_DOC = \
    """
    Byte value used in Water Extent files.  

    Note - legal (decimal) values are:

           0:  no water in pixel
           1:  no data (one or more bands) in source NBAR image
       2-127:  pixel masked for some reason (refer to MASKED bits)
         128:  water in pixel 

    Values 129-255 are illegal (i.e. if bit 7 set, all others must be unset)

    """

WATER_PRESENT         = 1 << 7   # (dec 128) bit 7: 1=water present, 0=no water if all other bits zero
MASKED_CLOUD          = 1 << 6   # (dec 64)  bit 6: 1=pixel masked out due to cloud, 0=unmasked
MASKED_CLOUD_SHADOW   = 1 << 5   # (dec 32)  bit 5: 1=pixel masked out due to cloud shadow, 0=unmasked
MASKED_HIGH_SLOPE     = 1 << 4   # (dec 16)  bit 4: 1=pixel masked out due to high slope, 0=unmasked
MASKED_TERRAIN_SHADOW = 1 << 3   # (dec 8)   bit 3: 1=pixel masked out due to terrain shadow or low solar incidence angle, 
0=unmasked
MASKED_SEA_WATER      = 1 << 2   # (dec 4)   bit 2: 1=pixel masked out due to being over sea, 0=unmasked
MASKED_NO_CONTIGUITY  = 1 << 1   # (dec 2)   bit 1: 1=pixel masked out due to lack of data contiguity, 0=unmasked
NO_DATA               = 1 << 0   # (dec 1)   bit 0: 1=pixel masked out due to NO_DATA in NBAR source, 0=valid data in NBAR
WATER_NOT_PRESENT     = 0        # (dec 0)          All bits zero indicated valid observation, no water present


SLOPE_THRESHOLD_DEGREES = 12.0           # Water detected on slopes equal or greater than this value are masked out 