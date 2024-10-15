# Load required libraries
library(knitr)
library(here)
library(tidyverse)
library(arrow)
library(gtsummary)
library(data.table)
library(collapse)

# Load the configuration utility
source("utils/config.R")

# Access configuration parameters
site_name <- config$site_name
tables_path <- config$tables_path
file_type <- config$file_type

# Print the configuration parameters
print(paste("Site Name:", site_name))
print(paste("Tables Path:", tables_path))
print(paste("File Type:", file_type))

# Set parameters
max_age_at_adm <- 119

# Define file paths
labs_filepath <- file.path(tables_path, paste0("clif_labs.", file_type))
labs_output_filepath <- file.path(here("output", "intermediate"), paste0("clif_labs_clean.", file_type))
labs_outlier_thresholds_filepath <- here("outlier-thresholds", "outlier_thresholds_labs.csv")

vitals_filepath <- file.path(tables_path, paste0("clif_vitals.", file_type))
vitals_output_filepath <- file.path(here("output", "intermediate"), paste0("clif_vitals_clean.", file_type))
vitals_outlier_thresholds_filepath <- here("outlier-thresholds", "nejm_outlier_thresholds_vitals.csv")

encounter_filepath <- file.path(tables_path, paste0("clif_encounter_demographics_dispo.", file_type))
encounter_output_filepath <- file.path(here("output", "intermediate"), paste0("clif_encounter_demographics_dispo_clean.", file_type))

# Specify directory for result files
results_path <- here("output")

##################### Functions  ###############################################
# Define function to read data
read_data <- function(filepath, filetype) {
  if (filetype == 'csv') {
    return(fread(filepath))
  } else if (filetype == 'parquet') {
    return(read_parquet(filepath))
  } else if (filetype == 'fst') {
    return(read_fst(filepath))
  } else {
    stop("Unsupported file type. Please provide either 'csv', 'parquet', or 'fst'.")
  }
}

# Define function to write data
write_data <- function(data, filepath, filetype) {
  if (filetype == 'csv') {
    fwrite(data, filepath)
  } else if (filetype == 'parquet') {
    write_parquet(data, filepath, compression = "SNAPPY")
  } else if (filetype == 'fst') {
    write_fst(data, filepath)
  } else {
    stop("Unsupported file type. Please provide either 'csv', 'parquet', or 'fst'.")
  }
}

# Define function to replace outliers with NA values (long format)
replace_outliers_with_na_long <- function(df, df_outlier_thresholds,
                                               category_variable, numeric_variable) {
  df <- df %>%
    left_join(df_outlier_thresholds, by = category_variable) %>%
    mutate(!!sym(numeric_variable) := ifelse(
      get(numeric_variable) < lower_limit | get(numeric_variable) > upper_limit,
      NA,
      get(numeric_variable)
    )) %>%
    select(-lower_limit, -upper_limit)
  
  return(df)
}

generate_summary_stats <- function(data, category_variable, numeric_variable) {
  summary_stats <- data %>%
    group_by({{ category_variable }}) %>%
    summarise(
      N = fsum(!is.na({{ numeric_variable }})),
      Min = fmin({{ numeric_variable }}, na.rm = TRUE),
      Max = fmax({{ numeric_variable }}, na.rm = TRUE),
      Mean = fmean({{ numeric_variable }}, na.rm = TRUE),
      Median = fmedian({{ numeric_variable }}, na.rm = TRUE),
      First_Quartile = fquantile({{ numeric_variable }}, 0.25, na.rm = TRUE),
      Third_Quartile = fquantile({{ numeric_variable }}, 0.75, na.rm = TRUE)
    ) %>%
    ungroup() %>%
    arrange(-desc({{ category_variable }}))
  
  return(summary_stats)
}

#####################     Labs   ###############################################
# Read labs data
clif_labs <- read_data(labs_filepath, file_type)
labs_outlier_thresholds <- read_data(labs_outlier_thresholds_filepath, 'csv')
dir_path <- file.path(results_path, 'labs')
dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

# if lab_value_numeric doesn't exist, create it
if (!"lab_value_numeric" %in% colnames(clif_labs)) {
  print("lab_value_numeric does not exist")
  if (is.character(clif_labs$lab_value)){
    clif_labs$lab_value_numeric <- as.numeric(parse_number(clif_labs$lab_value))
    print("lab_value_numeric created from character")
  }
  clif_labs$lab_value_numeric <- clif_labs$lab_value
    print("lab_value_numeric created from integer")
}

## replace outliers with NA
clif_labs_clean <- replace_outliers_with_na_long(clif_labs, 
                                                 labs_outlier_thresholds, 
                                                 'lab_category', 
                                                 'lab_value_numeric')

# Write clean labs file
write_data(clif_labs_clean, labs_output_filepath, file_type)

lab_summary_stats <- generate_summary_stats(clif_labs_clean,
                                            lab_category, 
                                            lab_value_numeric)
write_data(lab_summary_stats, file.path(results_path, "labs", paste0("clif_labs_summarystats_", site_name, ".csv")), 'csv')

#####################   Vitals   ###############################################

# Read vitals data
clif_vitals <- read_data(vitals_filepath, file_type)
vitals_outlier_thresholds <- read_data(vitals_outlier_thresholds_filepath, 'csv')
dir_path <- file.path(results_path, 'vitals')
dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

## replace outliers with NA
clif_vitals_clean <- replace_outliers_with_na_long(clif_vitals, 
                                                   vitals_outlier_thresholds, 
                                                   'vital_category', 
                                                   'vital_value')

# Write clean vitals file
write_data(clif_vitals_clean, vitals_output_filepath, file_type)

vital_summary_stats <- generate_summary_stats(clif_vitals_clean, 
                                              vital_category, 
                                              vital_value)
write_data(vital_summary_stats, file.path(results_path, "vitals", paste0("clif_vitals_summary_stats_", site_name, ".csv")), 'csv')

################Encounter Demographics Dispo  ##################################

clif_encounter <- read_data(encounter_filepath, file_type)

clif_encounter$age_at_admission <- ifelse(clif_encounter$age_at_admission > max_age_at_adm, 
                                          NA,
                                          clif_encounter$age_at_admission)

write_data(clif_encounter, encounter_output_filepath, file_type)
