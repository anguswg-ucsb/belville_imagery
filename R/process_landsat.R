# Angus Watters
# 01/10/2022
# Calculate Water indices from Satellite imagery

# ***************************
# ---- Extract/Load data ----
# ***************************

# directory containing TIFS
save_dir <- "D:/belville_tx/output_2023_01_12"

# Paths to TIF files
path_df <-
  data.frame(
    path      = list.files(save_dir, full.names = T),
    file_name = list.files(save_dir, full.names = F)
  ) %>%
  dplyr::tibble() %>%
  dplyr::mutate(
    basename  =  gsub('out_', "", gsub("\\..*","", file_name)),
    satellite =  sub("\\D*(\\d+).*", "\\1", basename)
  )

# ************************
# ---- Calculate NDWI ----
# ************************


# The Normalized Difference Water Index (NDWI) is derived from the Near-Infrared (NIR) and Green (G) channels. This formula highlights the amount of water in water bodies.
# NDWI = (Green - NIR)/(Green + NIR)

# make names for NDWI layers
stk_names <- paste0("ndwi_", path_df$basename)


ndwi_df <- ndwi_index()

# rm(i, rbands, nir_band, green_band, ndwi_name, ndwi, ndwi_lst)

# loop over each raster stack and extract bands 3 & 8 to calculate NDWI
ndwi_lst <- lapply(1:nrow(path_df), function(i) {


  # read in imagery raster bands
  rbands <- terra::rast(path_df$path[i])

  tryCatch({

    # band numbers
    green_band <- paste0("_B", ndwi_df[ndwi_df$sat_num == as.numeric(path_df$satellite[i]),]$green_num)
    nir_band   <- paste0("_B", ndwi_df[ndwi_df$sat_num == as.numeric(path_df$satellite[i]),]$nir_num)

    # make name for NDWI layer
    ndwi_name <- paste0("ndwi_", path_df$basename[i])

    message(paste0("Calculating NDWI - ", "[Sat: ",   as.numeric(path_df$satellite[i]),
                   " (",  gsub("_", "", green_band), ", ",   gsub("_", "", nir_band), ")] - ", i, "/", nrow(path_df)))

    # # Calculate NDWI = (Green - NIR)/(Green + NIR)
    ndwi <- (rbands[green_band] - rbands[nir_band]) / (rbands[green_band] +  rbands[nir_band])

    # assign name to NDWI layer
    ndwi <-
      ndwi %>%
      stats::setNames(c(stk_names[i]))

    ndwi

    },
    error = function(e) NULL
  )

}) %>%
  stats::setNames(stk_names) %>%
  terra::sds()

# Save out NDWI files
save_sds(
  sds            = ndwi_lst,
  save_directory = paste0("D:/belville_tx/ndwi_landsat_",  gsub('[^a-zA-Z0-9/_]+', '_', Sys.Date())),
  filenames      = names(ndwi_lst)
)
