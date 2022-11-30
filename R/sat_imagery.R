# Here is the site address:
# 6483 Highway 36 N
# Bellville, TX 77418

library(raster) # Raster Data handling
library(terra)
library(tidyverse) # Data Manipulation
library(getlandsat) # keyless Landsat data (2013-2017)
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
bellville_pt %>%
  mapview::mapview()

# Create a bounding box of area around point
bb <-
  bellville_pt %>%
  sf::st_buffer(4000) %>%
  sf::st_bbox()

# view bounding box area
bb  %>%
  mapview::mapview()
stac_path <- 'https://landsatlook.usgs.gov/stac-server' # Landsat STAC API Endpoint

# Call the STAC API endpoint
stac_json <-
  stac_path %>%
  httr::GET() %>%
  httr::content()

i <- 4

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
"{'limit': 400, 'bbox': [-97.56546020507812, 45.20332826663052, -97.2241973876953, 45.52751668442124]}"

params <-
  '{"limit":400, "bbox": [-97.56546020507812, 45.20332826663052, -97.2241973876953, 45.52751668442124]}' %>%
  jsonlite::toJSON()
stac_query <-
  stac_search %>%
  httr::POST(body = params)
stac_con <-
  stac_query %>%
  httr::content()
stac_final <- rawToChar(stac_con) %>% jsonlite::fromJSON()
stac_final$features
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
