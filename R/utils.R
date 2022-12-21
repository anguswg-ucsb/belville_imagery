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

  return(sub_index)

}

get_sentinal <- function(
    bq_project     = NULL,
    table_id       = NULL,
    bbox           = NULL,
    mask_shp       = NULL,
    bands          = NULL,
    start_date     = NULL,
    end_date       = NULL
) {

  # bq_project = "landsat-index-table"
  # table_id   = "bigquery-public-data.cloud_storage_geo_index.sentinel_2_index"
  # bbox       = bbox

  # mask_shp   =   data.frame(
  #                 lat = 30.007827243000914,
  #                 lng = -96.31442973176237
  #               ) %>%
  #                 sf::st_as_sf(
  #                   coords = c("lng", "lat"),
  #                   crs    = 4326
  #                 ) %>%
  #                 sf::st_buffer(1500) %>%
  #                 sf::st_bbox() %>%
  #                 sf::st_as_sfc() %>%
  #                 sf::st_sf() %>%
  #                 terra::vect()

  # terra::set.crs()
  # start_date = "2018-10-01"
  # end_date   = "2019-01-15"
  # bands = c(3, 8)

  # make mask shape to mask rasters to AOI Bounding box
  mask_shp <-
    bbox %>%
    sf::st_as_sfc() %>%
    sf::st_sf() %>%
    terra::vect()

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

  sub_index$uscene_id <- sapply(strsplit(sub_index$product_id, "_"), function(x) x[3])
  sub_index$utag_id   <- sapply(strsplit(sub_index$product_id, "_"), function(x) x[6])

  # extract and build URL dataframe
  url_df <-
    lapply(1:nrow(sub_index), function(y) {
      data.frame(
        url = gsub(
          "gs://",
          "https://storage.googleapis.com/",
          paste0(sub_index$base_url[y],
                 "/GRANULE/", sub_index$granule_id[y],
                 "/IMG_DATA/",sub_index$utag_id[y], "_" ,sub_index$uscene_id[y],  "_B",
                 ifelse(bands < 10, paste0("0", bands), bands),  ".jp2")
        )
      ) %>%
        dplyr::mutate(
          band        = ifelse(bands < 10, paste0("0", bands), bands),
          date        = sub_index$sensing_time[y],
          month       = lubridate::month(date),
          year        = lubridate::year(date),
          product_id  = sub_index$product_id[y],
          cloud_cover = sub_index$cloud_cover[y]
        ) %>%
        dplyr::relocate(product_id, date, cloud_cover, band, url)

    }) %>%
    dplyr::bind_rows() %>%
    dplyr::tibble() %>%
    # dplyr::group_by(product_id) %>%
    # dplyr::group_split()
    dplyr::group_by(band, month, year) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
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

          message(paste0("Product: ", url_df[[y]]$product_id[i], " (Band ", url_df[[y]]$band[i], ") - ",
                         y, "/",(length(url_df)*length(bands))))

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

      # # make sure masking shape is same projection as rasters
      # mask_shp <- terra::project(mask_shp, terra::crs(r_lst))
      #
      # # crop and mask rasters to AOI Bbox
      # r_lst <-
      #   r_lst %>%
      #   terra::crop(mask_shp) %>%
      #   terra::mask(mask_shp)

      # r_lst

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
