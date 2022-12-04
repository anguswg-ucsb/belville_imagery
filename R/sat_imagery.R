# Here is the site address:
# 6483 Highway 36 N
# Bellville, TX 77418

library(raster) # Raster Data handling
library(terra)
library(tidyverse) # Data Manipulation
# library(getlandsat) # keyless Landsat data (2013-2017)
library(sf) # Vector data processing
library(mapview) # Rapid Interactive visualization
library(rstac)

library(httr)
library(jsonlite)

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
