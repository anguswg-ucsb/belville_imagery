# Here is the site address:
# 6483 Highway 36 N Bellville, TX 77418

library(raster) # Raster Data handling
library(terra)
library(tidyverse) # Data Manipulation
# library(getlandsat) # keyless Landsat data (2013-2017)
library(sf) # Vector data processing
library(mapview) # Rapid Interactive visualization
library(rstac)

library(httr)
library(jsonlite)

library(bigrquery)

source("R/utils.R")
# path 26
# row 39

# ****************************************
# ---- Google Cloud Storage Landsat 8 ----
# ****************************************
# authorize BigQuery
bigrquery::bq_auth()

# List BigQuery Projects avaliable
bigrquery::bq_projects()

# path builder landsat di
sensor_id       <- "LC08"
collection_id   <- "01"
path    <- "26"
row     <- "39"
start_date      <- "2018-10-01"
end_date        <- "2018-12-31"

# rm(landsat_index, url_df, sub_index, sql, end_date, start_date, bq_project, table_id, row, path)
landsat_index <-
  get_landsat_index(
    bq_project =  "landsat-index-table",
    table_id   = "bigquery-public-data.cloud_storage_geo_index.landsat_index",
    path       = "26",
    row        = "39",
    start_date = "2018-09-01",
    end_date   = "2019-02-01"
  )

landsat_stk <-
  get_landsat(
    bq_project =  "landsat-index-table",
    table_id   = "bigquery-public-data.cloud_storage_geo_index.landsat_index",
    path       = "26",
    row        = "39",
    start_date = "2018-10-01",
    end_date   = "2019-01-15"
  )

# **************************************************
# ---- Calculate SWI/NDWI to locate waterbodies ----
# **************************************************

# bellville site location
bellville_pt <- data.frame(
  lat = 30.007827243000914,
  lng = -96.31442973176237
) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
  ) %>%
  sf::st_buffer(1000) %>%
  sf::st_bbox() %>%
  sf::st_as_sfc() %>%
  sf::st_as_sf() %>%
  sf::st_transform("+proj=utm +zone=14 +datum=WGS84 +units=m +no_defs") %>%
  terra::vect()

# Crop and Mask landsat rasters to AOI
landsat_mask <- lapply(1:length(landsat_stk), function(i){

  message(paste0("Cropping and masking landsat raster stack - ", i, "/", length(landsat_stk)))

  ls_mask <-
    landsat_stk[[i]] %>%
    terra::crop(bellville_pt) %>%
    terra::mask(bellville_pt)
  ls_mask

})

# Crop and Mask landsat rasters to AOI
ndwi_stk <- lapply(1:length(landsat_mask), function(i){

     message(paste0("Calculating NDWI landsat raster stack - ", i, "/", length(landsat_mask)))

    #   # Green and NIR bands
      green <- landsat_mask[[i]][grep("B3", names(landsat_mask[[i]]), value = T)]
      nir   <- landsat_mask[[i]][grep("B5", names(landsat_mask[[i]]), value = T)]

      # Calculate NDWI
      ndwi <- (green - nir)/(green + nir)

  ndwi
}) %>%
  terra::rast()

# view
# ndwi_stk %>% plot()
# ndwi_stk$LC08_L1TP_026039_20190113_20190131_01_T1_NDWI %>%
#   raster::raster() %>%
#   mapview::mapview()

# Calculate SWI
swi_stk <- lapply(1:length(landsat_mask), function(i){

  message(paste0("Calculating SWI landsat raster stack - ", i, "/", length(landsat_mask)))
  # 1/sqrt(blue(2) - SWIR1(6))
  # landsat 7: Band1 - band5

  # if Landsat 7
  if(grepl("LE07", names(landsat_mask[[i]])[1])) {

    # Blue and SWIR1 Bands
    blue    <- landsat_mask[[i]][grep("B1", names(landsat_mask[[i]]), value = T)]
    swir1   <- landsat_mask[[i]][grep("B5", names(landsat_mask[[i]]), value = T)]

    # Calculate NDWI
    swi     <- 1/(sqrt(blue - swir1))

    # change raster name
    names(swi) <- gsub("B1", "SWI", names(swi))

  }

  # if Landsat 8
  if(grepl("LC08", names(landsat_mask[[i]])[1])) {

    # Blue and SWIR1 Bands
    blue    <- landsat_mask[[i]][grep("B2", names(landsat_mask[[i]]), value = T)]
    swir1   <- landsat_mask[[i]][grep("B6", names(landsat_mask[[i]]), value = T)]

    # Calculate NDWI
    swi     <- 1/(sqrt(blue - swir1))

    # change raster name
    names(swi) <- gsub("B2", "SWI", names(swi))

  }

  swi

}) %>%
  terra::rast()


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
save_dir <- "D:/belville_tx/output"

# save individual TIFs
save_sds(
  sds            = sentinal_stk,
  save_directory = save_dir,
  filenames      = names(sentinal_stk)
  )

# ******************************
# ---- Landsat STAC catolog ----
# ******************************

# bellville site location
bellville_pt <- data.frame(
                    lat = 30.007827243000914,
                    lng = -96.31442973176237
                    ) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
    )

# Create a bounding box of area around point
bb <-
  bellville_pt %>%
  sf::st_buffer(4000) %>%
  sf::st_bbox()

stac_path <- 'https://landsatlook.usgs.gov/stac-server' # Landsat STAC API Endpoint

# Call the STAC API endpoint
stac_json <-
  stac_path %>%
  httr::GET() %>%
  httr::content()

# extract landsat collection list
landsat_collects <- sapply(1:length(stac_json), function(i) {

  if(stac_json$links[[i]]$rel == "data") {
    stac_json$links[[i]]$href
  }

  }) %>%
  purrr::compact() %>%
  unlist()

# call the endpoint and store the response
collects_res <-
  landsat_collects %>%
  httr::GET() %>%
  httr::content()

length(collects_res$collections)

# extract landsat collection list
stac_search <- sapply(1:length(stac_json), function(i) {

  if(stac_json$links[[i]]$rel == "search") {
    stac_json$links[[i]]$href
  }

}) %>%
  purrr::compact() %>%
  unlist()

# Post inputs
limit_return = "400"
xmin         = "-97.56546020507812"
ymin         = "45.20332826663052"
xmax         = "-97.2241973876953"
ymax         = "45.52751668442124"
date_time  <-  "2018-11-01T00:00:00Z/2018-12-05T23:59:59Z"

# Concantenate bounding box and limit_return into POST body JSON
post_val <- paste0('{"limit":', limit_return, '"bbox": [', xmin, ', ' , ymin, ', ', xmax, ', ', ymax, '] ', '"datetime":', date_time, '}')
# use the specified datetime format to create string variable of desired time range

# POST values
params <-
  post_val %>%
  jsonlite::toJSON()

# make POST query w/ bounding box
stac_query <-
  stac_search %>%
  httr::POST(body = params) %>%
  httr::content()

# convert raw to character and to JSON
stac_final <-
  stac_query %>%
  rawToChar() %>%
  jsonlite::fromJSON()

stac_links <-
  stac_final$features$links %>%
  dplyr::bind_rows() %>%
  dplyr::filter(rel == "self")
