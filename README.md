# CLIF Project Title

## Objective

Describe the project objective

## Required CLIF tables and fields

Please refer to the online [CLIF data dictionary](https://clif-consortium.github.io/website/data-dictionary.html), [ETL tools](https://github.com/clif-consortium/CLIF/tree/main/etl-to-clif-resources), and [specific table contacts](https://github.com/clif-consortium/CLIF?tab=readme-ov-file#relational-clif) for more information on constructing the required tables and fields. List all required tables for the project here, and provide a brief rationale for why they are required.

Example:

The following tables are required:

1. **patient**: `patient_id`, `race_category`, `ethnicity_category`, `sex_category`
2. **hospitalization**: `patient_id`, `hospitalization_id`, `admission_dttm`, `discharge_dttm`, `age_at_admission`
3. **medication_admin_continuous**: `hospitalization_id`, `admin_dttm`, `med_category`, `med_dose`
   - `med_category` = 'fentanyl', 'propofol', 'lorazepam', 'midazolam','hydromorphone','morphine'
4. **respiratory_support**: `hospitalization_id`, `recorded_dttm`, `device_category`
5. **patient_assessments**: `hospitalization_id`, `recorded_dttm`, `assessment_category`,`numerical_value`, `categorical_value`

## Cohort identification

Describe study cohort inclusion and exclusion criteria here

## Expected Results

Describe the output of the analysis. The final project results should be saved in the [`output/final`](output/README.md) directory.

## Detailed Instructions for running the project

## 1. Setup Project Environment

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

## 2. Update `config/config.json`

Follow instructions in the [config/README.md](config/README.md) file for detailed configuration steps.

## 3. Run code

Detailed instructions on the code workflow are provided in the [code directory](code/README.md)

---
