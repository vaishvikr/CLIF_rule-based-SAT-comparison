### General Workflow

Before start running code you will need to create a python evn to run the code.
Example for Python:

```
if Mac/Linux:
python3 -m venv .satsbt
source .satsbt/bin/activate
pip install -r requirements.txt

if Windows:
python -m venv .satsbt_ATS24
call .satsbt_ATS24\Scripts\activate.bat
pip install -r requirements.txt
```

1. Run the cohort_identification script ['code/01_cohort_id.ipynb'](code/01_cohort_id.ipynb)
   This script should:

   - Apply inclusion and exclusion criteria
   - Select required fields from each table
   - Filter tables to include only required observations

   Expected outputs:

   - A file will be created at ../output/intermediate/study_cohort.csv

2. Run the analysis script [`code/02_project_analysis.ipynb`](code/02_project_analysis.ipynb)
   This script should contain the main analysis code for the project.

   Input: ../output/intermediate/study_cohort.csv

   Output: One table1 file and One stats file [`output/final`](../output/README.md)
