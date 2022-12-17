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
  sql

  message(paste0("Querying BigQuery Sentinal 2 index table"))
  message(paste0("SQL query:\n", sql))

  # Query Index table from BigQuery Table
  sentinal_idx <- bigrquery::bq_project_query(
    x     = bq_project,
    query = sql
  )

  # Download subset of index to get desired product IDs
  sub_index <- bigrquery::bq_table_download(sentinal_idx)

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
