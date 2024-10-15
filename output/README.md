## Output directory

Use this directory to store intermediate or final results. 

* **The `final` folder should contain all the aggregate project results that will be delivered to the project PI via the specified aggregation method. Having all results in one folder will make exporting the contents more convenient**

* Files should have the naming syntax of [RESULT_NAME]_[SITE_NAME]_[SYSTEM_TIME].pdf. Use the config object to get the site name. For example, the below code exports a pdf file with the name Table_One_2024-10-04_UCMC.pdf to the results folder:

  R example:
  ```
  library(here)
  library(gtsummary)

  source("utils/config.R")

  table_one_hospitalization |> 
    as_gt() |> 
    gtsave(filename = here(paste0("results/Table_One_", Sys.Date(), "_", config$site_name, ".pdf")))
  ```

  Python example:
  ```
  import pandas as pd
  from gt import GTTable
  from pathlib import Path
  from datetime import datetime

  # Load configuration from  config module
  from utils import config

  # Assuming table_one_hospitalization is a pandas DataFrame
  # Convert the DataFrame to a GTTable object (as_gt equivalent)
  table_gt = GTTable.from_pandas(table_one_hospitalization)

  # Create a file path using the 'pathlib' module
  output_file = Path("results") / f"Table_One_{datetime.today().date()}_{config.site_name}.pdf"

  # Save the GTTable object to a PDF file (gtsave equivalent)
  table_gt.to_pdf(output_file)
  ```