 ## Code directory

Update this README with the specific project workflow instructions.
This directory contains scripts for the project workflow. The general workflow consists of three main steps: cohort identification, quality control, and analysis. Scripts can be implemented in R or Python, depending on project requirements. Please note that this workflow is just a suggestion, and you may change the structure to suit your project needs.

### General Workflow

1. Run the cohort_identification script
   This script should:
   - Apply inclusion and exclusion criteria
   - Select required fields from each table
   - Filter tables to include only required observations

   Expected outputs:
   - cohort_ids: a list of unique identifiers for the study cohort
   - cohort_data: the filtered study cohort data
   - cohort_summary: a summary table describing the study cohort

   Examples of cohort identification scripts:
   - [`code/templates/Python/01_cohort_identification_template.py`](templates/Python/01_cohort_identification_template.py)
   - [`code/templates/R/01_cohort_identification_template.R`](templates/R/01_cohort_identification_template.R)

2. Run the quality_control script
   This script should:
   - Perform project-specific quality control checks on the filtered cohort data
   - Handle outliers using predefined thresholds as given in `outlier-thresholds` directory. 
   - Clean and preprocess the data for analysis

   Script: [`code/templates/R/02_project_quality_checks_template.R`](templates/R/02_project_quality_checks_template.R) & [`code/templates/R/03_outlier_handling_template.R`](templates/R/03_outlier_handling_template.R) 

   Input: cohort_data 

   Output: cleaned_cohort_data 

3. Run the analysis script(s)
   This script (or set of scripts) should contain the main analysis code for the project.
   It may be broken down into multiple scripts if necessary.
   
   Script: [`code/templates/R/04_project_analysis_template.R`](templates/R/04_project_analysis_template.R) 

   Input: cleaned_cohort_data 

   Output: [List of expected result files, e.g., statistical_results, figures, tables saved in the [`output/final`](../output/README.md) directory] 



