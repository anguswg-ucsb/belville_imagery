# Here is the site address:
# 6483 Highway 36 N Bellville, TX 77418

library(raster) # Raster Data handling
library(terra)
library(tidyverse) # Data Manipulation
library(sf) # Vector data processing
library(mapview) # Rapid Interactive visualization
library(httr)
library(jsonlite)
library(bigrquery)

source("R/utils.R")

# ******************
# ---- Load AOI ----
# ******************

# bellville site bounding box
bbox <- data.frame(
  lat = 30.007827243000914,
  lng = -96.31442973176237
) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
  ) %>%
  sf::st_buffer(1200) %>%
  sf::st_bbox()

# ****************************************
# ---- Google Cloud Storage Landsat 8 ----
# ****************************************
# authorize BigQuery
bigrquery::bq_auth()

# List BigQuery Projects avaliable
bigrquery::bq_projects()

# dates of interest
start_date      <- "1990-01-01"
end_date        <- "2023-01-01"

# pull relevant landsat tiles from Google Bigquery index
landsat_index <-
  get_landsat_index(
    bq_project =  "landsat-index-table",
    table_id   = "bigquery-public-data.cloud_storage_geo_index.landsat_index",
    path       = "26",
    row        = "39",
    start_date = start_date,
    end_date   = end_date
  )

# get Landsat but NDWI bands only
ndwi_stk <- get_landsat_ndwi(
  index_tbl = landsat_index,
  mask_shp  = bbox
)

# Save SDS TIFs
save_sds(
  sds            = ndwi_stk,
  save_directory = paste0("D:/belville_tx/output_",  gsub('[^a-zA-Z0-9/_]+', '_', Sys.Date())),
  filenames      = names(ndwi_stk)
)

# ****************************
# ---- Sentinal 2 imagery ----
# ****************************

# bellville site bounding box
bbox <- data.frame(
    lat = 30.007827243000914,
    lng = -96.31442973176237
  ) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
  ) %>%
  sf::st_buffer(2000) %>%
  sf::st_bbox()

# mask_shp <- bbox

# Query BigQuery index table containing sentinal 2 scenes
sentinal_idx <- get_sentinal_index(
  bq_project = "landsat-index-table",
  table_id   = "bigquery-public-data.cloud_storage_geo_index.sentinel_2_index",
  bbox       = bbox,
  start_date = "2015-09-01",
  end_date   = "2022-12-01"
)

# remove S2A granule
subset_idx <-
  sentinal_idx %>%
  dplyr::filter(!grepl("S2A_OPER", granule_id))

# downloads Sentinal 2 bands 3 and 8 and crop/mask to AOI
sentinal_stk <- get_sentinal(
  index_tbl      = subset_idx,
  bands          = c(3, 8),
  mask_shp       = bbox
)

# directory to save TIFs
save_dir <- "D:/belville_tx/ndwi_output"

# save individual TIFs
save_sds(
  sds            = sentinal_stk,
  save_directory = save_dir,
  filenames      = names(sentinal_stk)
  )

# ******************************
# ******************************
# ******************************

