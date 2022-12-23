# Angus Watters
# 12/23/2022
# Calculate Water indices from Satellite imagery

# ***************************
# ---- Extract/Load data ----
# ***************************

# directory containing TIFS
save_dir <- "D:/belville_tx/output"

# Paths to TIF files
path_df <-
  data.frame(
    path      = list.files(save_dir, full.names = T),
    file_name = list.files(save_dir, full.names = F)
    ) %>%
  dplyr::tibble() %>%
  dplyr::mutate(
    basename =  gsub('out_', "", gsub("\\..*","",file_name))
  ) %>%
  tidyr::separate(col = basename, into = c("date", "id_num"))

# ************************
# ---- Calculate NDWI ----
# ************************


# The Normalized Difference Water Index (NDWI) is derived from the Near-Infrared (NIR) and Green (G) channels. This formula highlights the amount of water in water bodies.
# NDWI = (Green - NIR)/(Green + NIR)

# make names for NDWI layers
stk_names <- paste0("ndwi_", path_df$date)
# rm(stk_names, ndwi_lst)

# loop over each raster stack and extract bands 3 & 8 to calculate NDWI
ndwi_lst <- lapply(1:nrow(path_df), function(i) {


  # read in imagery raster bands
  rbands <- terra::rast(path_df$path[i])

  # make name for NDWI layer
  ndwi_name <- paste0("ndwi_", path_df$date[i], "_", path_df$id_num[i])

  message(paste0("Calculating NDWI - ", i, "/", nrow(path_df)))

  # Calculate NDWI = (Green - NIR)/(Green + NIR)
  ndwi <- (rbands["_B03"] - rbands["_B08"]) / (rbands["_B03"] +  rbands["_B08"])

  # assign name to NDWI layer
  ndwi <-
    ndwi %>%
    stats::setNames(c(stk_names[i]))

  ndwi

}) %>%
  stats::setNames(stk_names) %>%
  terra::sds()
ndwi_lst$ndwi_20151005 %>%
ndwi_lst$ndwi_20151005_1
# Save out NDWI files
save_sds(
    sds            = ndwi_lst,
    save_directory = "D:/belville_tx/ndwi_output",
    filenames      = names(ndwi_lst)
)

rm(tmp)
s <- rast(system.file("ex/logo.tif", package="terra"))
plot(s)
s

tmp <-
  ndwi_lst[[1:25]] %>%
  terra::rast()

terra::nlayer(s)
terra::nlyr(s)
animate(tmp, n=1)
# ***************************
# ---- Extract/Load data ----
# ***************************
