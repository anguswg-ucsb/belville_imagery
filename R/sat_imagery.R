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
# landsat_path    <- "44"
# landsat_row     <- "34"
start_date      <- "2018-10-01"
end_date        <- "2018-12-31"
# start_date      <- "2013-03-30"
# end_date        <- "2017-03-10"

# link to scene
scene_id        <- paste0(sensor_id, "_L1GT_", landsat_path, landsat_row, "_", gsub("-", "", start_date), "_", gsub("-", "", end_date)  ,"_01_T2")
scene_path      <- paste0("gs://gcp-public-data-landsat/",  sensor_id, "/", collection_id, "/", landsat_path, "/", landsat_row, "/", scene_id, "/")
message('SELECT * FROM `bigquery-public-data.cloud_storage_geo_index.landsat_index` WHERE wrs_path = 26 AND wrs_row = 39 AND date_acquired >= "2018-10-01" AND date_acquired <= "2019-01-15"')
# BigQuery Project ID
bq_proj_id <- "bigquery-public-data.cloud_storage_geo_index.landsat_index"

library(bigrquery)
library(terra)
library(dplyr)
get_landsat_index <- function(
    bq_project     = NULL,
    table_id       = NULL,
    path           = NULL,
    row            = NULL,
    start_date     = NULL,
    end_date       = NULL
) {

  # check if valid BigQuery Project ID is given
  if(is.null(bq_project)) {
    stop(paste0("Please enter a valid BigQuery Project String"))

  }

  # check if valid BigQuery table ID given
  if(is.null(table_id)) {
    stop(paste0("Please enter a valid BigQuery Table ID "))

  }

  # if no end_date is given, default to current date
  if(is.null(end_date)) {
    end_date <- Sys.Date()
  }

  # if no start date is given, default to 30 days before end date
  if(is.null(start_date)) {
    start_date <- end_date - as.difftime(30, unit = "days")
  }

  if(any(is.null(path), is.null(row))) {
    stop(paste0("Please enter a valid scene 'path' and 'row'"))
  }

  # Construct SQL Query
  sql <- paste0(
    "SELECT * FROM `", 	table_id, "` WHERE wrs_path = ", path, " AND wrs_row = ", row,
    ' AND date_acquired >= "' , start_date, '"',
    ' AND date_acquired <= "', end_date, '"'
  )

  message(paste0("Querying BigQuery Landsat index table"))
  message(paste0("SQL query:\n", sql))

  # Query Index table from BigQuery Table
  landsat_index <- bigrquery::bq_project_query(
    x     = bq_project,
    query = sql
  )

  # Download subset of index to get desired product IDs
  sub_index <- bigrquery::bq_table_download(landsat_index)

  # extract and build URL dataframe
  url_df <-
    lapply(1:nrow(sub_index), function(y) {
      data.frame(
        url = gsub(
          "gs://",
          "https://storage.googleapis.com/",
          paste0(sub_index$base_url[y], "/", paste0(sub_index$product_id[y], "_B", 1:8, ".TIF")),
        )) %>%
        dplyr::mutate(
          band        = paste0("B", 1:8),
          date        = sub_index$date_acquired[y],
          product_id  = sub_index$product_id[y],
          cloud_cover = sub_index$cloud_cover[y]
        ) %>%
        dplyr::relocate(product_id, date, cloud_cover, band, url)
    }) %>%
    dplyr::bind_rows() %>%
    dplyr::tibble()

  return(url_df)

}

get_landsat <- function(
    bq_project     = NULL,
    table_id       = NULL,
    path           = NULL,
    row            = NULL,
    start_date     = NULL,
    end_date       = NULL
    ) {
  # bq_project =  "landsat-index-table"
  # table_id   = "bigquery-public-data.cloud_storage_geo_index.landsat_index"
  # path       = "26"
  # row        = "39"
  # start_date = "2018-10-01"
  # end_date   = "2019-01-15"
  # check if valid BigQuery Project ID is given
  if(is.null(bq_project)) {
    stop(paste0("Please enter a valid BigQuery Project String"))

  }

  # check if valid BigQuery table ID given
  if(is.null(table_id)) {
    stop(paste0("Please enter a valid BigQuery Table ID "))

  }

  # if no end_date is given, default to current date
  if(is.null(end_date)) {
    end_date <- Sys.Date()
  }

  # if no start date is given, default to 30 days before end date
  if(is.null(start_date)) {
    start_date <- end_date - as.difftime(30, unit = "days")
  }

  if(any(is.null(path), is.null(row))) {
    stop(paste0("Please enter a valid scene 'path' and 'row'"))
  }

  # Construct SQL Query
  sql <- paste0(
    "SELECT * FROM `", 	table_id, "` WHERE wrs_path = ", path, " AND wrs_row = ", row,
    ' AND date_acquired >= "' , start_date, '"',
    ' AND date_acquired <= "', end_date, '"'
  )

  message(paste0("Querying BigQuery Landsat index table"))
  message(paste0("SQL query:\n", sql))

  # Query Index table from BigQuery Table
  landsat_index <- bigrquery::bq_project_query(
    x     = bq_project,
    query = sql
  )

  # Download subset of index to get desired product IDs
  sub_index <- bigrquery::bq_table_download(landsat_index)

  # extract and build URL dataframe
  url_df <-
    lapply(1:nrow(sub_index), function(y) {
      data.frame(
        url = gsub(
          "gs://",
          "https://storage.googleapis.com/",
          paste0(sub_index$base_url[y], "/", paste0(sub_index$product_id[y], "_B", 1:7, ".TIF")),
        )) %>%
        dplyr::mutate(
          band        = paste0("B", 1:7),
          date        = sub_index$date_acquired[y],
          product_id  = sub_index$product_id[y],
          cloud_cover = sub_index$cloud_cover[y]
        ) %>%
        dplyr::relocate(product_id, date, cloud_cover, band, url)
    }) %>%
    dplyr::bind_rows() %>%
    dplyr::tibble() %>%
    dplyr::group_by(product_id) %>%
    dplyr::group_split()

  message(paste0("Downloading LANDSAT data..."))
  # y = 1
  # rm(r_lst, y, ls_stk)
  ls_stk <- lapply(1:length(url_df), function(y) {

    prod_id <- unique(url_df[[y]]$product_id)

    # download rasters
    r_lst <-
      lapply(1:nrow(url_df[[y]]), function(i) {
        message(paste0("Product: ", url_df[[y]]$product_id[i], " (", url_df[[y]]$band[i], ")" ))
        tryCatch(
          terra::rast(url_df[[y]]$url[i]),
          error = function(e) message(paste0("ERROR\nCould not find product: ",
                                             url_df[[y]]$product_id[i], "(", url_df[[y]]$band[i], ")" ))
          )
      }) %>%
      terra::rast()

      })

  # ls_stk  %>%
  #   terra::sds()
  # # Get TIFs from Google Cloud Storage Landsat bucket
  # landsat_stk <-
  #   lapply(1:nrow(url_df), function(y) {
  #     message(paste0("Product: ", url_df$product_id[y], " (", url_df$band[y], ")" ))
  #     tryCatch(
  #       terra::rast(url_df$url[y]),
  #       error = function(e) message(paste0("ERROR\nCould not find product: ", url_df$product_id[y], "(", url_df$band[y], ")" ))
  #     )
  #   }) %>%
  #   purrr::compact()

  return(ls_stk)

}

# rm(landsat_index, url_df, sub_index, sql, end_date, start_date, bq_project, table_id, row, path)
landsat_index <-
  get_landsat_index(
    bq_project =  "landsat-index-table",
    table_id   = "bigquery-public-data.cloud_storage_geo_index.landsat_index",
    path       = "26",
    row        = "39",
    start_date = "2018-10-01",
    end_date   = "2019-01-15"
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

# Green - NIR/ Green + NIR
green   <- landsat_stk[[1]][grep("B3", names(landsat_stk[[1]]), value = T)]
nir     <- landsat_stk[[1]][grep("B5", names(landsat_stk[[1]]), value = T)]

ndwi <- (green - nir)/(green + nir)

#  # extract bands of interest (B1, B2, B3)
# water_index <- lapply(1:length(landsat_stk), function(x) {
#   names(landsat_stk[[x]])
# }) %>%
#   unlist() %>%
#   grepl("B2|B3|B4", .)
#
# # Subset :Landsat rasters to bands of interest
# water_stk <- landsat_stk[water_index]
#
# #  RGB Colors from Landsat Multiband
# blue  <- water_stk[[1]]
# green <- water_stk[[2]]
# red   <- water_stk[[3]]
#
# # stack RGB raster stack
# rgb = c(red, green, blue)
#
# # Plot RGB Bands
# plotRGB(rgb)

# ***********************
# ---- Subset to AOI ----
# ***********************

# bellville site location
bellville_pt <- data.frame(
  lat = 30.007827243000914,
  lng = -96.31442973176237
) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
  ) %>%
  sf::st_buffer(2000) %>%
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

rm(green, ndwi, nir,ndwi_stk,  i)
# i = 2
# Crop and Mask landsat rasters to AOI
ndwi_stk <- lapply(1:length(landsat_mask), function(i){

     message(paste0("Calculating NDWI landsat raster stack - ", i, "/", length(landsat_mask)))

    # if Landsat 7
    if(grepl("LE07", names(landsat_mask[[i]])[1])) {

      # Green and NIR bands
      green <- landsat_mask[[i]][grep("B2", names(landsat_mask[[i]]), value = T)]
      nir   <- landsat_mask[[i]][grep("B4", names(landsat_mask[[i]]), value = T)]

      # Calculate NDWI
      ndwi <- (green - nir)/(green + nir)

      # change raster name
      names(ndwi) <- gsub("B2", "NDWI", names(ndwi))
    }

    # if Landsat 8
    if(grepl("LC08", names(landsat_mask[[i]])[1])) {

      # Green and NIR bands
      green <- landsat_mask[[i]][grep("B3", names(landsat_mask[[i]]), value = T)]
      nir   <- landsat_mask[[i]][grep("B5", names(landsat_mask[[i]]), value = T)]

      # Calculate NDWI
      ndwi <- (green - nir)/(green + nir)

      # change raster name
      names(ndwi) <- gsub("B3", "NDWI", names(ndwi))
    }

  ndwi
}) %>%
  terra::rast()

ndwi_stk %>% plot()
# i <- 2
rm(i, blue, swir1, swi, swi_stk)
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

plot(swi_stk)

tmp1 <- swi_stk %>%
  raster::stack()

# mapview::mapview(tmp1$LC08_L1TP_026039_20190113_20190131_01_T1_SWI)
# +
  mapview::mapview(tmp1$LE07_L1TP_026039_20181220_20190115_01_T1_SWI)
swi_stk %>%
  raster::stack() %>%
  mapview::mapview()
# 1/sqrt(blue(2) - SWIR1(6))
# band1 - band5
ndwi_stk %>% plot()
# ndwi_stk$%>% plot()
# Green - NIR/ Green + NIR
green   <- landsat_stk[[1]][grep("B3", names(landsat_stk[[1]]), value = T)]
nir     <- landsat_stk[[1]][grep("B5", names(landsat_stk[[1]]), value = T)]

ndwi <- (green - nir)/(green + nir)


tmp_sf <- sf::st_as_sf(bellville_pt)
tmp_r <- landsat_mask[[1]]$LC08_L1TP_026039_20190113_20190131_01_T1_B6 %>%
  raster::raster()

  mapview::mapview(tmp_r) +
  mapview::mapview(tmp_sf)
# landsa
rgb_mask <-
  rgb %>%
  terra::crop(bellville_pt) %>%
  terra::mask(bellville_pt)

plot(rgb_mask)


terra::crs(landsat_stk[[1]], proj = T)



plot(bellville_pt)

# view Bellville point
# bellville_pt %>% mapview::mapview()

# Create a bounding box of area around point
bb <-
  bellville_pt %>%
  sf::st_buffer(4000) %>%
  sf::st_bbox()


# path           = NULL
# row            = NULL
path    <- "26"
row     <- "39"
start_date     = NULL
end_date       = NULL


# Construct SQL Query
sql <- paste0(
  "SELECT * FROM `", 	bq_proj_id, "` WHERE wrs_path = ", landsat_path, " AND wrs_row = ", landsat_row,
  ' AND date_acquired >= "' , start_date, '"',
  ' AND date_acquired <= "', end_date, '"'
  )
message(sql)

# Query Index table from BigQuery Table
landsat_index <- bigrquery::bq_project_query(
  x     = "landsat-index-table",
  query = sql
  )

# Subset of Landsat Index table to find the exact product ID we need
sub_index <- bigrquery::bq_table_download(landsat_index)

# sub_index <-bq_tbl
#   bq_tbl %>%
#   dplyr::filter(
#     date_acquired >= start_date,
#     date_acquired <= end_date
#   ) %>%
#   dplyr::select(scene_id, product_id, spacecraft_id, date_acquired, cloud_cover, base_url)
#
# sub_index$base_url


# extract and build URL dataframe
url_df <-
  lapply(1:nrow(sub_index), function(y) {
    data.frame(
      url = gsub(
        "gs://",
        "https://storage.googleapis.com/",
        paste0(sub_index$base_url[y], "/", paste0(sub_index$product_id[y], "_B", 1:8, ".TIF")),
        )) %>%
      dplyr::mutate(
        band        = paste0("B", 1:8),
        date        = sub_index$date_acquired[y],
        product_id  = sub_index$product_id[y],
        cloud_cover = sub_index$cloud_cover[y]
        ) %>%
      dplyr::relocate(product_id, date, cloud_cover, band, url)
    }) %>%
  dplyr::bind_rows() %>%
  dplyr::tibble()

message(paste0("Downloading LANDSAT data..."))

landsat_stk <-
  lapply(1:nrow(url_df), function(y) {
        message(paste0("Product ID: ", url_df$product_id[y], "\nBand: ", url_df$band[y] ))
        tryCatch(
          terra::rast(url_df$url[y]),
          error = function(e) message(paste0("ERROR\nCould not find product ID: ", url_df$product_id[y]))
          )
        })


landsat_stk[[18]] %>% plot()

tmp <- landsat_stk[[18]]
tmp[[1]]
library(httr)
r <- raster::raster(url_df$url[2])
r <- terra::rast(url_df$url[2])
plot(r)
rast <- httr::GET(url = url_df$url[91])

library(jsonlite)
jsonlite::fromJSON(httr::content(rast))

library(raster)

y = 2
lapply(1:nrow(url_df), function(y) {
  r <- raster::raster(url_df$url[20])
})
r <- raster::raster(url_df$url[1])
res_con <- httr::content(rast)

res_con$node
rast$content
url_df$url[1]

  # as.vector()
unique(sub_index$scene_id)
sub_urls
paste0(sub_index$base_url, "_B", 1:8, ".TIF")
sub_index$base_url[1]
sub_index$base_url[1]

paste0(sub_index$base_url[1], "_B", 1:8, ".TIF")
# /SENSOR_ID/01/PATH/ROW/SCENE_ID/
#
#   The components of this path are:
#
#   SENSOR_ID: An identifier for the particular satellite and camera sensor.
# 01: An indicator that the data is part of Landsat Collection 1.
# PATH: The WRS path number.
# ROW: The WRS row number.
# SCENE_ID: The unique scene ID.
# As an example, one Landsat 8 scene over California can be found here:
#
#   gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/

scene_id        <- paste0(sensor_id, "_L1GT_", landsat_path, landsat_row, "_", gsub("-", "", start_date), "_", gsub("-", "", end_date)  ,"_01_T2")
# scene_path      <- paste0("gs://gcp-public-data-landsat/",  sensor_id, "/", collection_id, "/", landsat_path, "/", landsat_row, "/", scene_id, "/")
scene_path      <- paste0("gs://gcp-public-data-landsat/",  sensor_id, "/", collection_id, "/", landsat_path, "/", landsat_row, "/", scene_id, "/")
"gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/"
"gs://gcp-public-data-landsat/LE07/01/026/039/LE07_L1GT_026039_20181017_20181114_01_T2/"

paste0("_B", 1:8, ".TIF")

index_file <- "gs://gcp-public-data-landsat/index.csv.gz"
# bellville site location
bellville_pt <- data.frame(
                    lat = 30.007827243000914,
                    lng = -96.31442973176237
                    ) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
    )

# view Bellville point
# bellville_pt %>% mapview::mapview()

# Create a bounding box of area around point
bb <-
  bellville_pt %>%
  sf::st_buffer(4000) %>%
  sf::st_bbox()



# view bounding box area
# bb  %>%
#   mapview::mapview()
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

httr::POST()
library(sf)
sf::st_bbox()

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
stac_final$features
stac_final$links
stac_final$features$assets$coastal$alternate$s3
stac_links <-
  stac_final$features$links %>%
  dplyr::bind_rows() %>%
  dplyr::filter(rel == "self")

stac_links$href[1]
"https://landsatlook.usgs.gov/stac-server/collections/landsat-c2l1/items/LC09_L1GT_008001_20221201_20221201_02_T2.tif"
stac_links[[1]]
# %>%
  # jsonlite::fromJSON()
stac_query$request
# jsonlite::toJSON(params) %>% class()
# params %>% class()
# params <-'{"a":1,"b":{}}'
  # stac_json$links[[i]]$rel
  # stac_json

# }
stac_json$links[[21]]$rel
  # jsonlite::fromJSON()
stac_response$
stac_response
json_path <- "https://landsat-stac.s3.amazonaws.com/catalog.json"

library(jsonlite)

stac_json <- jsonlite::fromJSON(json_path)
stac_json

stac_path <- "https://landsat-stac.s3.amazonaws.com/"
s_obj <- rstac::stac(stac_path)
rstac::stac_search()
req_stac <- rstac::get_request(s_obj)

# get list of Landsat 8 scenes
scenes <- getlandsat::lsat_scenes()

names(scenes)
bb[[1]]
bb
sub_scenes <-
  scenes %>%
  dplyr::filter(
    min_lat >= bb[[2]],
    # max_lat >= bb[[4]],
    min_lon <= bb[[1]],
    max_lon >= bb[[3]]
  )
bb
scenes
