"""
# Sentinel-2_nrt process env configs read from variables if environment specific
# - ows pod setup configurations
# - SQS name
# - Process configuration
# - - archival
# - - - product names and archival condition
# - - indexing
# - - - product names and indexing condition
# - - web application mv updates
# - - - explorer update product names
# - - - ows update product names
"""


# TODO: The archive condition is for sentinel_2_nrt, set different condition for different products
ARCHIVE_PRODUCTS = "s2a_nrt_granule s2b_nrt_granule"
ARCHIVE_CONDITION = "[$(date -d '-365 day' +%F), $(date -d '-93 day' +%F)]"


# products to be indexed
# TODO: This list need to be split when multiple SQS queues are setup for different products
INDEXING_PRODUCTS = (
    "s2a_nrt_granule",
    "s2b_nrt_granule",
    "ga_s2a_ard_nbar_granule",
    "ga_s2b_ard_nbar_granule",
    "wofs_albers",
    "ls5_fc_albers",
    "ls7_fc_albers",
    "ls8_fc_albers",
)
# S3 Record list for indexing products
# TODO: This list need to be split when multiple SQS queues are setup for different products
PRODUCT_RECORD_PATHS = (
    "L2/sentinel-2-nrt/S2MSIARD/*/*/ARD-METADATA.yaml",
    "L2/sentinel-2-nbar/S2MSIARD_NBAR/*/*/ARD-METADATA.yaml",
    "WOfS/WOFLs/v2.1.5/combined/*/*/*/*/*/*.yaml",
    "fractional-cover/fc/v2.2.1/*/*/*/*/*/*/*.yaml",
)
# ows layer product to be updated
UPDATE_EXTENT_PRODUCTS = (
    "s2_nrt_granule_nbar_t",
    "wofs_albers",
    "fc_albers_combined",
    "s2_ard_granule_nbar_t",
)

# batch indexing s3 paths
S2_NRT_S3_PATHS = "s3://dea-public-data/L2/sentinel-2-nrt/S2MSIARD/**/ARD-METADATA.yaml"
