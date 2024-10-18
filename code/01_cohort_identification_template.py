# %%
import pandas as pd
import numpy as np
import duckdb
import pyCLIF as pc

# %% [markdown]
# ### Base Population

# %%
hosp = pc.load_data('clif_hospitalization')
pat = pc.load_data('clif_patient')
rst = pc.load_data('clif_respiratory_support')

hosp = hosp[
    (hosp['admission_dttm'].dt.year >= 2020) &
    (hosp['admission_dttm'].dt.year <= 2024) &
    (hosp['hospitalization_id'].isin(rst[rst['device_category']=='IMV'].hospitalization_id.unique())) 
].reset_index(drop=True)

required_id= hosp['hospitalization_id'].unique()
print(len(required_id),' : potential cohort count')

base = pd.merge(hosp,pat,on='patient_id',how='inner')\
[['patient_id', 'hospitalization_id','admission_dttm', 'discharge_dttm','age_at_admission', 'discharge_category','sex_category','race_category', 'ethnicity_category']]

base['admission_dttm'] = pc.getdttm(base['admission_dttm'])

base.columns

del hosp,pat

# %% [markdown]
# ### Resp

# %%
rst_col = [ 'hospitalization_id', 'recorded_dttm', 'device_category', 'mode_category']
rst = rst[rst['hospitalization_id'].isin(required_id)][rst_col].reset_index(drop=True)
rst['recorded_dttm'] = pc.getdttm(rst['recorded_dttm'])

# %% [markdown]
# ### MAC

# %%
mac = pc.load_data('clif_medication_admin_continuous')
mac_col = ['hospitalization_id', 'admin_dttm','med_dose', 'med_dose_unit','med_category','med_group']
mac = mac[(mac['hospitalization_id'].isin(required_id)) & (mac['med_group']=='sedation')][mac_col].reset_index(drop=True)
mac['admin_dttm'] = pc.getdttm(mac['admin_dttm'])

# %% [markdown]
# ### Patient_assessment

# %%
pat_assess_cats_rquired = ['cam_icu_total',
                            'cpot_body_movement',
                            'cpot_compliance_vent',
                            'cpot_facial',
                            'cpot_muscle_tension',
                            'cpot_total',
                            'cpot_vocalization ',
                            'dvprs',
                            'poss',
                            'rass',
                            'sat_delivery_anxiety',
                            'sat_delivery_cardiac_arrhthymia',
                            'sat_delivery_icp',
                            'sat_delivery_increased_rr',
                            'sat_delivery_pass_fail',
                            'sat_delivery_resp_distress',
                            'sat_delivery_spo2',
                            'sat_screen_comfort_care',
                            'sat_screen_continous_sedation',
                            'sat_screen_etoh',
                            'sat_screen_icp',
                            'sat_screen_increasing_agitation',
                            'sat_screen_mi',
                            'sat_screen_nm_blockade',
                            'sat_screen_pass_fail',
                            'sat_screen_rass_goal',
                            'sat_screen_seizures ',
                            'sbt_delivery_no_resp_distress']

pat_at = pc.load_data('clif_patient_assessments',-1)
pat_at_col = ['hospitalization_id', 'recorded_dttm','assessment_value', 'assessment_category']
pat_at = pat_at[(pat_at['hospitalization_id'].isin(required_id)) & (pat_at['assessment_category'].isin(pat_assess_cats_rquired)) ][pat_at_col].reset_index(drop=True)
pat_at['recorded_dttm'] = pc.getdttm(pat_at['recorded_dttm'])

# %% [markdown]
# ### WIde Dataset

# %%
duckdb.register("base", base)
duckdb.register("pat_at", pat_at)
duckdb.register("rst", rst)
duckdb.register("mac", mac)

q="""
WITH
    uni_event_dttm as (
        select distinct
            hospitalization_id,
            time_line
        from
            (
                SELECT
                    hospitalization_id,
                    recorded_dttm AS time_line
                FROM
                    rst
                where
                    recorded_dttm is not null
                UNION
                SELECT
                    hospitalization_id,
                    recorded_dttm AS time_line
                FROM
                    pat_at
                where
                    recorded_dttm is not null
                UNION
                SELECT
                    hospitalization_id,
                    admin_dttm AS time_line
                FROM
                    mac
                where
                    admin_dttm is not null
            ) uni_time
    )
select distinct
    patient_id,
    a.hospitalization_id,
    admission_dttm,
    discharge_dttm,
    age_at_admission,
    discharge_category,
    sex_category,
    race_category,
    ethnicity_category,
    time_line
from
    base a
    left join uni_event_dttm b on a.hospitalization_id = b.hospitalization_id
"""
wide_cohort_df = duckdb.sql(q).df()
pc.deftime(wide_cohort_df['time_line'])

# %%
query = """
WITH pas_data AS (
    SELECT  distinct assessment_value ,	assessment_category	,
    hospitalization_id || '_' || strftime(recorded_dttm, '%Y%m%d%H%M') AS combo_id
    FROM pat_at where recorded_dttm is not null 
) 
PIVOT pas_data
ON assessment_category
USING first(assessment_value)
GROUP BY combo_id
"""
p_pas = duckdb.sql(query).df()

query = """
WITH mac_data AS (
    SELECT  distinct med_dose ,	med_category	,
    hospitalization_id || '_' || strftime(admin_dttm, '%Y%m%d%H%M') AS combo_id
    FROM mac where admin_dttm is not null 
) 
PIVOT mac_data
ON med_category
USING min(med_dose)
GROUP BY combo_id
"""
p_mac = duckdb.sql(query).df()

# %%
duckdb.register("expanded_df", wide_cohort_df)
duckdb.register("p_pas", p_pas)
duckdb.register("p_mac", p_mac)

q="""
    WITH 
    u_rst as (select *, hospitalization_id || '_' || strftime(recorded_dttm, '%Y%m%d%H%M') AS combo_id from rst),

    u_expanded_df as (select *, hospitalization_id || '_' || strftime(time_line, '%Y%m%d%H%M') AS combo_id from expanded_df)

    select * from  u_expanded_df a

        left join u_rst e on a.combo_id=e.combo_id 

            left join p_mac g on a.combo_id=g.combo_id 
                        
                left join p_pas h on a.combo_id=h.combo_id 
"""

all_join_df = duckdb.sql(q).df().drop_duplicates()

# %%
if all_join_df.shape[0] != wide_cohort_df.shape[0]:
    print('Data has duplicates or same timestamp, contact project owner')

# %%
all_join_df.to_csv('output/study_cohort.csv', index=False)

# %%



