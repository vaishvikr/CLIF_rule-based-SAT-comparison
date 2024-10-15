# Cohort identification script for inpatient admissions

# Load required libraries
library(knitr)
library(here)
library(tidyverse)
library(arrow)
library(gtsummary)

# Objective: identify a cohort of hospitalizations from CLIF tables
# Identify patients admitted to the hospital in a given date range. 
# Export a list of `hospitalization_id` and filtered CLIF tables for the 
# identified hospitalizations.
# An example project for this cohort would be included for surveillance of 
# sepsis events based on the CDC Adult Sepsis Event criteria.

# Specify inpatient cohort parameters

## Date range
start_date <- "2020-01-01"
end_date <- "2021-12-31"

## Inclusion and exclusion criteria
include_pediatric <- FALSE
include_er_deaths <- TRUE

# TO DO: develop these criteria further

# Specify required CLIF tables

# List of all table names from the CLIF 2.0 ERD
tables <- c("patient", "hospitalization", "vitals", "labs", 
            "medication_admin_continuous", "adt", "patient_assessments",
            "respiratory_support", "position", "dialysis", "intake_output", 
            "ecmo_mcs", "procedures", "admission_diagnosis", "provider", 
            "sensitivity", "medication_orders", "medication_admin_intermittent",
            "therapy_details", "microbiology_culture", "sensitivity", 
            "microbiology_nonculture")

# Tables that should be set to TRUE for this project
true_tables <- c("patient", "hospitalization", "adt", 
                 #"medication_admin_intermittent", 
                 "microbiology_culture", 
                 "vitals", "labs", "medication_admin_continuous", 
                 "respiratory_support")

# Create a named vector and set the boolean values
table_flags <- setNames(tables %in% true_tables, tables)

# Load the required CLIF tables into memory and return an error if a required 
# table is missing

## Specify CLIF table location in your repository

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

# List all CLIF files in the directory
clif_table_filenames <- list.files(path = tables_path, 
                                   pattern = paste0("^clif_.*\\.", file_type, "$"), 
                                   full.names = TRUE)

# Extract the base names of the files (without extension)
clif_table_basenames <- basename(clif_table_filenames) %>%
  str_remove(paste0("\\.", file_type, "$"))

# Create a lookup table for required files based on table_flags
required_files <- paste0("clif_", names(table_flags)[table_flags])

# Check if all required files are present
missing_tables <- setdiff(required_files, clif_table_basenames)
if (length(missing_tables) > 0) {
    stop(paste("Missing required tables:", 
               paste(missing_tables, collapse = ", ")))
}

# Filter only the filenames that are required
required_filenames <- clif_table_filenames[clif_table_basenames %in% required_files] 

# Read the required files into a list of data frames
if (file_type == "parquet") {
  data_list <- lapply(required_filenames, function(file) {
    open_dataset(file) %>%
      #filter(your_filter_condition) %>% # add a filter condition for all tables
      collect()
  })
} else if (file_type == "csv") {
  data_list <- lapply(required_filenames, read_csv)
} else if (file_type == "fst") {
  data_list <- lapply(required_filenames, read.fst)
} else {
  stop("Unsupported file format")
}

# Assign the data frames to variables based on their file names
for (i in seq_along(required_filenames)) {
  # Extract the base name of the file (without extension)
  object_name <- str_remove(basename(required_filenames[i]),
                            paste0("\\.", file_type, "$"))
  # Make the object name valid for R (replace with underscores)
  object_name <- make.names(object_name)
  # Assign the tibble to a variable with the name of the file
  assign(object_name, data_list[[i]])
}

remove(data_list)

# Identify hospital admissions for the specified date range with at least one
# `location_category` of `c("Ward", "ICU")`

clif_hospitalization_filtered <- clif_hospitalization %>%
  filter(admission_dttm >= start_date & admission_dttm <= end_date)

if (!include_pediatric) {
  clif_hospitalization_filtered <- clif_hospitalization_filtered %>%
    filter(age_at_admission >= 18)
}

inpatient_hospitalization_ids <- clif_adt %>%
  filter(location_category %in% c("Ward", "ICU")) %>%
  select(hospitalization_id) %>%
  unique() %>%
  pull(hospitalization_id)

cohort_hospitalization_ids <- clif_hospitalization_filtered %>%
  filter(hospitalization_id %in% inpatient_hospitalization_ids) %>%
  select(hospitalization_id) %>%
  pull(hospitalization_id)

## Identify patients who died in the ER and include if specified

if (include_er_deaths) {
  # identify hospitalization_ids with only ER location_category
  ER_only_hospitalization_ids <- clif_adt %>%
    group_by(hospitalization_id) %>%
    filter(all(location_category == "ER")) %>%
    pull(hospitalization_id)

  ER_death_ids <- clif_hospitalization %>%
    filter(hospitalization_id %in% ER_only_hospitalization_ids) %>%
    filter(discharge_category == "Expired") %>%
    pull(hospitalization_id)

  cohort_hospitalization_ids <- union(cohort_hospitalization_ids, ER_death_ids)
}

## Export the list of `hospitalization_id` for the identified patients

save(cohort_hospitalization_ids, 
     file = here("study_cohort/01_cohort_hospitalization_ids.RData"))

## Filter the CLIF tables for the identified hospitalizations

filter_clif_table <- function(table, filter_col, cohort_ids, select_cols = NULL) {
  filtered_table <- table %>%
    filter(!!sym(filter_col) %in% cohort_ids)
  
  # Optionally select relevant columns
  if (!is.null(select_cols)) {
    filtered_table <- filtered_table %>%
      select(all_of(select_cols))
  }
  
  return(filtered_table)
}

#remove the patient table from the list of tables to filter
table_flags["patient"] <- FALSE

# Filter the required tables for the identified hospitalizations
for (table_name in names(table_flags)[table_flags]) {
  # Dynamically construct the full table name (e.g., "clif_labs", "clif_vitals")
  full_table_name <- paste0("clif_", table_name)
  
  # Assign the filtered result to a new variable with "_cohort" suffix
  assign(
    paste0(full_table_name, "_cohort"), 
    filter_clif_table(get(full_table_name), "hospitalization_id", 
                      cohort_hospitalization_ids)
  )
}

# Filter the patient table for the identified patients
cohort_patient_ids <- clif_hospitalization_filtered %>%
  select(patient_id) %>%
  unique() %>%
  pull(patient_id)

clif_patient_cohort <- filter_clif_table(clif_patient, "patient_id", 
                                         cohort_patient_ids)

# TO DO:
# filter tables, e.g. `labs` down to only the labs that are relevant for the project

## Save all filtered tables to the `study_cohort` folder

save(list = ls(pattern = "clif_.*_cohort"), 
     file = here("study_cohort/01_clif_cohort_tables.RData"))

# Create a table 1 of patient demographics for the cohort

admits_per_patient <- clif_hospitalization_cohort %>%
  group_by(patient_id) %>%
  summarise(n_hospitalizations = n())

table_one_patient <- clif_patient_cohort %>%
  left_join(admits_per_patient, by = "patient_id") %>%
  mutate(age =  as.Date(start_date) -as.Date(birth_date),
         age = as.numeric(age, units = "days") / 365.25) %>%
  ungroup() %>%
  select(age, sex_category, race_category, ethnicity_category, 
         n_hospitalizations, language_name) %>%
  tbl_summary()

print(table_one_patient)

# Create table 1 of hospitalization level data for the cohort

ever_icu <- clif_adt_cohort %>%
  filter(location_category == "ICU") %>%
  select(hospitalization_id) %>%
  mutate(ever_icu = 1) %>%
  unique()

table_one_hospitalization <- clif_hospitalization_cohort %>%
  mutate(length_of_stay = as.numeric(as.Date(discharge_dttm) - 
                                     as.Date(admission_dttm), units = "days")) %>%
  select(patient_id, hospitalization_id, age_at_admission, discharge_category, 
         admission_type_name, length_of_stay) %>%
  left_join(clif_patient_cohort %>% 
              select(patient_id, race_category, sex_category, 
                     ethnicity_category, language_name)) %>% 
  left_join(ever_icu, by = "hospitalization_id") %>%
  mutate(ever_icu = ifelse(is.na(ever_icu), 0, 1)) %>%
  select(-patient_id, - hospitalization_id) %>%
  tbl_summary(by = ever_icu)

#export the table with gtsummary
table_one_hospitalization %>% 
  as_gt() %>% 
  gt::gtsave(filename = here(paste0("output/intermediate/Table_One_", Sys.Date(), "_", 
                                    config$site_name, ".pdf")))