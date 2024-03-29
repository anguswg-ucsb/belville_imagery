#' Retrieve climate data and process zonal climate timeseries for a spatial polygon object
#' @param shp a spatial polygon object (sf or sp) with a unique "grid_id" column
#' @param param a meteorological parameter (see 'param_meta$terraclim')
#' @param start_date a start date given as "YYYY-MM-DD"
#' @param end_date an end date given as "YYYY-MM-DD"
#'
#' @return dataframe with a timeseries of param values for each polygon in shp
#' @export
shp_ts <- function(
    shp,
    param,
    start_date,
    end_date
) {

  # check if a unique grid_id column is present in shp
  if(!any(grepl("grid_id", names(shp)) == TRUE)) {
    shp <-
      shp %>%
      dplyr::mutate(
        grid_id = 1:dplyr::n()
      )
  }

  # pull climate data for area, param, and date range
  clim <- climateR::getTerraClim(
    AOI       = shp,
    param     = param,
    startDate = start_date,
    endDate   = end_date
  )

  doParallel::stopImplicitCluster()

  # select raster stack and execute zonal
  clim_ts <-
    clim[[1]] %>%
    terra::rast() %>%
    zonal::execute_zonal(
      data = .,
      geom = shp,
      ID   = "grid_id",
      join = FALSE

    )

  # clean up dates and climate variable
  clim_ts <-
    clim_ts %>%
    dplyr::select(grid_id, contains("mean")) %>%
    sf::st_drop_geometry() %>%
    dplyr::tibble() %>%
    tidyr::pivot_longer(cols = c(-grid_id)) %>%
    dplyr::mutate(
      name = paste0(gsub("\\.", "-", gsub("mean.X", "", name)), "-01")
    ) %>%
    dplyr::group_by(grid_id) %>%
    stats::setNames(c("grid_id", "date", param))

  return(clim_ts)

}

#' Retrieve climate data and process zonal climate timeseries for a spatial polygon object
#' @param shp a spatial polygon object (sf or sp) with a unique "grid_id" column
#' @param param a meteorological parameter (see 'param_meta$terraclim')
#' @param start_date a start date given as "YYYY-MM-DD"
#' @param end_date an end date given as "YYYY-MM-DD"
#'
#' @return dataframe with a timeseries of param values for each polygon in shp
#' @export
pt_ts <- function(
    pt,
    param,
    start_date,
    end_date
) {

  # pull climate data for area, param, and date range
  clim_ts <- climateR::getGridMET(
    AOI       = pt,
    param     = param,
    startDate = start_date,
    endDate   = end_date
  )

  doParallel::stopImplicitCluster()

  return(clim_ts)

}


aggreg_gridmet <- function(
    pt,
    start_date,
    end_date,
    ignore_params = NULL,
    verbose = TRUE
) {

  # list of gridMET parameters
  param_lst <- climateR::param_meta$gridmet$common.name
  desc_lst  <- climateR::param_meta$gridmet$description

  # specifically ignore certain climate variables
  if(!is.null(ignore_params)) {

    param_lst <- param_lst[param_lst != ignore_params]
    desc_lst  <- desc_lst[param_lst != ignore_params]

    }
  if(verbose == TRUE) {

    message(paste0("Aggregating climate data per point..."))

  }

  # iterate over gridMET params and get all climate data variables for a point
  climate_ts <- lapply(1:length(param_lst), function(i){

    if(verbose == TRUE) {
      message(paste0("\nVariable: ", desc_lst[i], " (", param_lst[i], ")",
                     "\nStart date: ", start_date,
                     "\nEnd date: ", end_date
      ))
    }

    # pull climate dataset
    clim_ts <- pt_ts(
      pt         = pt,
      param      = param_lst[i],
      start_date = start_date,
      end_date   = end_date
    )  %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))

    clim_ts

  })

  # reduce list of dataframes to single dataframe
  climate_ts <-
    climate_ts %>%
    purrr::reduce(
      full_join,
      by = c("lat", "lon", "source", "date")
    )

  return(climate_ts)

}

aggreg_climate <- function(
    shp,
    start_date,
    end_date,
    verbose = TRUE
) {

  # list of TerraClim parameters
  param_lst <- climateR::param_meta$terraclim$common.name
  desc_lst  <- climateR::param_meta$terraclim$description

  if(verbose == TRUE) {
    message(paste0("Aggregating climate data per polygon..."))
  }

  # iterate over TerrClim params and get climate data than generate timeseries per polygon
  climate_ts <- lapply(1:length(param_lst), function(i){

    if(verbose == TRUE) {
      message(paste0("\nVariable: ", desc_lst[i], " (", param_lst[i], ")",
                     "\nStart date: ", start_date,
                     "\nEnd date: ", end_date
      ))
    }

    # pull climate dataset
    clim_ts <- shp_ts(
      shp        = shp,
      param      = param_lst[i],
      start_date = start_date,
      end_date   = end_date
    )

    clim_ts

  })

  # reduce list of dataframes to single dataframe
  climate_ts <-
    climate_ts %>%
    purrr::reduce(
      full_join,
      by = c("grid_id", "date")
      # by = c("row", "column", "date")
    )

  return(climate_ts)

}
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
    dplyr::tibble() %>%
    dplyr::arrange(date)

  return(url_df)

}

ndwi_index <- function() {

  band_df <- data.frame(
    satellite  = c("l5", "l7", "l8", "s2"),
    sat_num    = c(5, 7, 8, 2),
    green      = c("B2", "B2", "B3", "B3"),
    nir        = c("B5", "B5", "B5", "B8"),
    green_num  = c(2, 2, 3, 3),
    nir_num    = c(5, 5, 5, 8)
  )

  return(band_df)

}

get_landsat_ndwi <- function(
    index_tbl      = NULL,
    mask_shp       = NULL

) {
  # check if mask shape is a SF Object
  if(any(grepl("sf", class(mask_shp)))) {

    # check if mask shape is a SF Polygon or Multipolygon
    if(sf::st_is(mask_shp, "POLYGON") | sf::st_is(mask_shp, "MULTIPOLYGON") == TRUE) {

      mask_shp <-
        mask_shp %>%
        terra::vect()

      message(paste0("Converting SF Polygon/Multipolygon to Terra SpatVector"))

    } else {

      stop(paste0("Please provide an SF Polygon/Multipolygon or a Terra SpatVector"))

    }

  }

  # check if mask shape is a bbox Object
  if(class(mask_shp) == 'bbox') {

    # make mask shape to mask rasters to AOI Bounding box
    mask_shp <-
      mask_shp %>%
      sf::st_as_sfc() %>%
      sf::st_sf() %>%
      terra::vect()
  }

  # NDWI index dataframe for different landsat launch bands
  ndwi_df <- ndwi_index()

  # unique product IDs
  uproduct_ids <- unique(index_tbl$product_id)

  #  extract and build URL dataframe, select NDWI bands
  url_df <-
    lapply(1:length(uproduct_ids), function(y) {

      # subset index
      sub_idx <- index_tbl[index_tbl$product_id == uproduct_ids[y],]

      # iteration satellite
      sat       <- sub("\\D*(\\d+).*", "\\1",   sub_idx$product_id[1])

      # relevant bands
      rel_bands <- ndwi_df[ndwi_df$sat_num == as.numeric(sat), ]

      # list of bands
      bands <- paste0("B", c(rel_bands$green_num[1],  rel_bands$nir_num[1]))

      # subset index to bands of interest
      sub_idx <- sub_idx[sub_idx$band %in% bands, ]
      sub_idx$satellite <- sat
      sub_idx

    }) %>%
    dplyr::bind_rows() %>%
    dplyr::tibble() %>%
    dplyr::group_by(satellite) %>%
    dplyr::arrange(date, .by_group = T) %>%
    dplyr::ungroup() %>%
    split(factor(.$product_id, levels = unique(.$product_id)))

  message(paste0("Downloading Landsat data..."))

  # dates to assign as names to list
  stk_dates <- lapply(1:length(url_df), function(z) {
    gsub(
      "-",
      "_",
      paste0(
        as.character(unique(url_df[[z]]$satellite)), "_", as.character(as.Date(unique(url_df[[z]]$date))),
        "_", as.character(unique(url_df[[z]]$product_id))
      )
    )
  }) %>%
    unlist()

  # loop over list of URLs pointing to Sentinal data in GCP
  ls_stk <- lapply(1:length(url_df), function(y) {

    prod_id <- unique(url_df[[y]]$product_id)

    # download rasters
    r_lst <-
      lapply(1:nrow(url_df[[y]]), function(i) {

        message(paste0(
          "Product: ", url_df[[y]]$product_id[i], " (Band ", url_df[[y]]$band[i], ") - ",
          y, "/", (length(url_df))))

        tryCatch(
          terra::rast(url_df[[y]]$url[i]),

          error = function(e) NULL
        )
      })

    # if NULL elemnts in list, return NULL
    if(!is.null(unlist(r_lst))) {

      message("---> Cropping and masking to AOI")

      r_lst <-
        r_lst %>%
        terra::rast() %>%
        terra::crop(terra::project(mask_shp, terra::crs(r_lst[[1]]))) %>%
        terra::mask(terra::project(mask_shp, terra::crs(r_lst[[1]])))

      r_lst

    } else {

      r_lst <- NULL

      r_lst

    }

  })

  # remove missing dates from name list
  stk_dates <- stk_dates[!sapply(ls_stk,is.null)]

  # remove NULL list elements
  ls_stk <- ls_stk[!sapply(ls_stk,is.null)]

  # Set names and make a terra sds
  ls_stk <-
    ls_stk %>%
    stats::setNames(stk_dates) %>%
    terra::sds()


  return(ls_stk)

}

# get_landsat_ndwi2 <- function(
#     index_tbl      = NULL,
#     mask_shp       = NULL
#
# ) {
#   # check if mask shape is a SF Object
#   if(any(grepl("sf", class(mask_shp)))) {
#
#     # check if mask shape is a SF Polygon or Multipolygon
#     if(sf::st_is(mask_shp, "POLYGON") | sf::st_is(mask_shp, "MULTIPOLYGON") == TRUE) {
#
#       mask_shp <-
#         mask_shp %>%
#         terra::vect()
#
#       message(paste0("Converting SF Polygon/Multipolygon to Terra SpatVector"))
#
#     } else {
#
#       stop(paste0("Please provide an SF Polygon/Multipolygon or a Terra SpatVector"))
#
#     }
#
#   }
#
#   # check if mask shape is a bbox Object
#   if(class(mask_shp) == 'bbox') {
#
#     # make mask shape to mask rasters to AOI Bounding box
#     mask_shp <-
#       mask_shp %>%
#       sf::st_as_sfc() %>%
#       sf::st_sf() %>%
#       terra::vect()
#   }
#
#   # NDWI index dataframe for different landsat launch bands
#   ndwi_df <- ndwi_index()
#
#   # unique product IDs
#   uproduct_ids <- unique(index_tbl$product_id)
#
#   #  extract and build URL dataframe, select NDWI bands
#   url_df <-
#     lapply(1:length(uproduct_ids), function(y) {
#
#           # subset index
#           sub_idx <- index_tbl[index_tbl$product_id == uproduct_ids[y],]
#
#           # iteration satellite
#           sat       <- sub("\\D*(\\d+).*", "\\1",   sub_idx$product_id[1])
#
#           # relevant bands
#           rel_bands <- ndwi_df[ndwi_df$sat_num == as.numeric(sat), ]
#
#           # list of bands
#           bands <- paste0("B", c(rel_bands$green_num[1],  rel_bands$nir_num[1]))
#
#           # subset index to bands of interest
#           sub_idx <- sub_idx[sub_idx$band %in% bands, ]
#           sub_idx$satellite <- sat
#           sub_idx
#
#           # data.frame( url  = gsub( "gs://","https://storage.googleapis.com/", paste0(
#           #       index_tbl$url[y], "/", paste0(index_tbl$product_id[y], "_B",
#           #       ifelse(bands < 10, paste0("0", bands), bands),".TIF")) ),
#           #   date = index_tbl$date[y],satellite = sat,  product_id = index_tbl$product_id[y], band= paste0("B",
#             #   ifelse(bands < 10, paste0("0", bands), bands)), cloud_cover   = index_tbl$cloud_cover[y])
#       }) %>%
#       dplyr::bind_rows() %>%
#       dplyr::tibble() %>%
#       dplyr::group_by(satellite) %>%
#       dplyr::arrange(date, .by_group = T) %>%
#       dplyr::ungroup() %>%
#       split(factor(.$product_id, levels = unique(.$product_id)))
#
#     message(paste0("Downloading Landsat data..."))
#
#     # dates to assign as names to list
#     stk_dates <- lapply(1:length(url_df), function(z) {
#       gsub(
#         "-",
#         "_",
#         paste0(
#           as.character(unique(url_df[[z]]$satellite)), "_", as.character(as.Date(unique(url_df[[z]]$date))),
#           "_", as.character(unique(url_df[[z]]$product_id))
#                )
#         )
#     }) %>%
#       unlist()
#
#   # loop over list of URLs pointing to Sentinal data in GCP
#   ls_stk <- lapply(1:length(url_df), function(y) {
#
#       prod_id <- unique(url_df[[y]]$product_id)
#
#       # download rasters
#       r_lst <-
#         lapply(1:nrow(url_df[[y]]), function(i) {
#
#           message(paste0(
#             "Product: ", url_df[[y]]$product_id[i], " (Band ", url_df[[y]]$band[i], ") - ",
#             y, "/", (length(url_df))))
#
#           tryCatch(
#             terra::rast(url_df[[y]]$url[i]),
#
#             error = function(e) NULL
#             # message(paste0("ERROR\nCould not find product: ", url_df[[y]]$product_id[i], "(", url_df[[y]]$band[i], ")" ))
#           )
#         })
#
#       # if NULL elemnts in list, return NULL
#       if(!is.null(unlist(r_lst))) {
#
#         message("---> Cropping and masking to AOI")
#
#         r_lst <-
#           r_lst %>%
#           terra::rast() %>%
#           terra::crop(terra::project(mask_shp, terra::crs(r_lst[[1]]))) %>%
#           terra::mask(terra::project(mask_shp, terra::crs(r_lst[[1]])))
#
#         r_lst
#
#       } else {
#
#         r_lst <- NULL
#
#         r_lst
#
#       }
#
#   })
#
#   # remove missing dates from name list
#   stk_dates <- stk_dates[!sapply(ls_stk,is.null)]
#
#   # remove NULL list elements
#   ls_stk <- ls_stk[!sapply(ls_stk,is.null)]
#
#   # Set names and make a terra sds
#   ls_stk <-
#     ls_stk %>%
#     stats::setNames(stk_dates) %>%
#     terra::sds()
#
#
#   return(ls_stk)
#
# }

get_landsat <- function(
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

  return(ls_stk)

}

get_sentinal_index <- function(
    bq_project     = NULL,
    table_id       = NULL,
    bbox           = NULL,
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

  if(is.null(bbox)) {
    stop(paste0("Please enter a valid 'bbox' bounding box object  'path' and 'row'"))
  }

  north_lat <- bbox["ymax"]
  south_lat <- bbox["ymin"]
  west_lon  <- bbox["xmax"]
  east_lon  <- bbox["xmin"]

  # Construct SQL Query
  sql <- paste0("SELECT *  FROM `", 	table_id,
                "` WHERE ",
                south_lat, " >= south_lat AND ",
                north_lat, " <= north_lat AND ",
                west_lon, " >= west_lon AND ",
                east_lon, " <= east_lon",
                ' AND sensing_time >= "' , start_date, '"',
                ' AND sensing_time <= "', end_date, '"'
  )

  message(paste0("Querying BigQuery Sentinal 2 index table"))
  message(paste0("SQL query:\n", sql))

  # Query Index table from BigQuery Table
  sentinal_idx <- bigrquery::bq_project_query(
    x     = bq_project,
    query = sql
  )

  # Download subset of index to get desired product IDs
  sub_index <- bigrquery::bq_table_download(sentinal_idx)

  # add unique scene ID and unique tag id for use downloading downstream
  sub_index$uscene_id <- sapply(strsplit(sub_index$product_id, "_"), function(x) x[3])
  sub_index$utag_id   <- sapply(strsplit(sub_index$product_id, "_"), function(x) x[6])

  return(sub_index)

}

get_sentinal <- function(
    index_tbl      = NULL,
    bands          = NULL,
    mask_shp       = NULL
) {

    # check if mask shape is a SF Object
    if(any(grepl("sf", class(mask_shp)))) {

      # check if mask shape is a SF Polygon or Multipolygon
      if(sf::st_is(mask_shp, "POLYGON") | sf::st_is(mask_shp, "MULTIPOLYGON") == TRUE) {

        mask_shp <-
          mask_shp %>%
          terra::vect()

        message(paste0("Converting SF Polygon/Multipolygon to Terra SpatVector"))

      } else {

        stop(paste0("Please provide an SF Polygon/Multipolygon or a Terra SpatVector"))

      }

    }

    # check if mask shape is a bbox Object
    if(class(mask_shp) == 'bbox') {

      # make mask shape to mask rasters to AOI Bounding box
      mask_shp <-
        mask_shp %>%
        sf::st_as_sfc() %>%
        sf::st_sf() %>%
        terra::vect()


    }

  # extract and build URL dataframe
  url_df <-
    lapply(1:nrow(index_tbl), function(y) {
      data.frame(
        url = gsub(
          "gs://",
          "https://storage.googleapis.com/",
          paste0(index_tbl$base_url[y],
                 "/GRANULE/", index_tbl$granule_id[y],
                 "/IMG_DATA/",index_tbl$utag_id[y], "_" ,index_tbl$uscene_id[y],  "_B",
                 ifelse(bands < 10, paste0("0", bands), bands),  ".jp2")
        )
      ) %>%
        dplyr::mutate(
          band        = ifelse(bands < 10, paste0("0", bands), bands),
          date        = index_tbl$sensing_time[y],
          # month       = lubridate::month(date),
          # year        = lubridate::year(date),
          product_id  = index_tbl$product_id[y],
          cloud_cover = index_tbl$cloud_cover[y]
        ) %>%
        dplyr::relocate(product_id, date, cloud_cover, band, url)

    }) %>%
    dplyr::bind_rows() %>%
    dplyr::tibble() %>%
    dplyr::group_by(product_id) %>%
    dplyr::group_split()

  message(paste0("Downloading Sentinal 2 data..."))

  # dates to assign as names to list
  stk_dates <- lapply(1:length(url_df), function(y) {

    as.character(as.Date(unique(url_df[[y]]$date)))

  }) %>%
    unlist()


  # loop over list of URLs pointing to Sentinal data in GCP
  sentinal_stk <- lapply(1:length(url_df), function(y) {

      prod_id <- unique(url_df[[y]]$product_id)

      # download rasters
      r_lst <-
        lapply(1:nrow(url_df[[y]]), function(i) {

          message(paste0(
                  "Product: ", url_df[[y]]$product_id[i], " (Band ", url_df[[y]]$band[i], ") - ",
                  y, "/", (length(url_df))))

          tryCatch(
            terra::rast(url_df[[y]]$url[i]),

            error = function(e) NULL
              # message(paste0("ERROR\nCould not find product: ", url_df[[y]]$product_id[i], "(", url_df[[y]]$band[i], ")" ))
          )
        })

      # if NULL elemnts in list, return NULL
      if(!is.null(unlist(r_lst))) {

        message(paste0("Cropping and masking - ", unique(url_df[[y]]$product_id)))

        r_lst <-
          r_lst %>%
          terra::rast() %>%
          terra::crop(terra::project(mask_shp, terra::crs(r_lst[[1]]))) %>%
          terra::mask(terra::project(mask_shp, terra::crs(r_lst[[1]])))

        r_lst

      } else {

        r_lst <- NULL

        r_lst

      }

    })
  # %>% stats::setNames(stk_dates)
  # %>% terra::sds()

  # remove missing dates from name list
  stk_dates <- stk_dates[!sapply(sentinal_stk,is.null)]

  # remove NULL list elements
  sentinal_stk <- sentinal_stk[!sapply(sentinal_stk,is.null)]

  # Set names and make a terra sds
  sentinal_stk <-
    sentinal_stk %>%
    stats::setNames(stk_dates) %>%
    terra::sds()


  return(sentinal_stk)

}


save_sds <- function(
    sds,
    save_directory,
    filenames
    ) {

  # loop over stack and save TIFs individually
  for (i in 1:length(sds)) {

    # # directory store
    # save_path <- paste0(save_directory, "/output")

    if(!dir.exists(save_directory)) {

      message(paste0("Creating output directory"))

      dir.create(save_directory)

    }

    message(paste0("Saving tif - ", i, "/", length(sds)))

    # save raster
    terra::writeRaster(
      sds[[i]],
      paste0(save_directory, "/out_", gsub('[^a-zA-Z0-9/_]+', '', filenames[i]), "_", i, ".tif")
    )

  }

}
