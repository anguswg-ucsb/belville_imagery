library(sf)
library(climateR)
library(raster)
library(terra)
library(exactextractr)
library(mapview)
library(zonal)
library(tidyverse)
library(doParallel)

source("R/utils.R")

# bellville site bounding box
shp <- data.frame(
  lat = 30.007827243000914,
  lng = -96.31442973176237
) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
  ) %>%
  sf::st_buffer(1200) %>%
  sf::st_bbox() %>%
  sf::st_as_sfc() %>%
  sf::st_as_sf()

mapview::mapview(shp)


pt <- data.frame(
  lat = 30.007827243000914,
  lng = -96.31442973176237
) %>%
  sf::st_as_sf(
    coords = c("lng", "lat"),
    crs    = 4326
  )

# aggregate all climate variables per each polygon
pt_climate <- aggreg_gridmet(
  pt         = pt,
  start_date = "1979-01-01",
  end_date   = "2023-01-22",
  ignore_params = c("palmer"),
  verbose    = TRUE
)

# save oout CSV
readr::write_csv(
  pt_climate,
  "D:/belville_tx/climate/belville_daily_gridmet_climate.csv"
)

param_df <- climateR::param_meta$gridmet

# save out parameter definitions dataframe
readr::write_csv(
  param_df,
  "D:/belville_tx/climate/gridmet_climate_parameters.csv"
)


# Model grid polygon with unique "grid_id" column
grid <-
  grid_path %>%
  sf::read_sf() %>%
  dplyr::mutate(
    grid_id = paste0(row, "_", column)
  )

# aggregate all climate variables per each polygon
climate_ts <- aggreg_climate(
  shp        = shp,
  start_date = "1959-01-01",
  end_date   = "2020-12-01",
  verbose    = TRUE
)

# save oout CSV
readr::write_csv(
  dplyr::select(climate_ts, -grid_id),
  "D:/belville_tx/climate/belville_monthly_climate_data.csv"
)

param_df <- climateR::param_meta$terraclim

# save out parameter definitions dataframe
readr::write_csv(
  param_df,
  "D:/belville_tx/climate/climate_parameters.csv"
)

