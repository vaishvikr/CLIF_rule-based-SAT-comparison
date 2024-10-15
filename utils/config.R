# Load necessary libraries
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  install.packages("jsonlite")
}

library(jsonlite)

# Function to load configuration
load_config <- function() {
  json_path <- "config/config.json"
  if (file.exists(json_path)) {
    config <- fromJSON(json_path)
    message("Loaded configuration from config.json")
  } else {
    stop("Configuration file not found. Please create config.json",
         "based on the config_template.")
  }
  return(config)
}

# Load the configuration
config <- load_config()
