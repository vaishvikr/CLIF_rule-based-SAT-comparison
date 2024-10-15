## Outlier Thresholds

Update the outlier thresholds in this directory for variables specific to the project. This ensures that the values are not hard-coded in the coding scripts. 
The outlier thresholds can be stored in various formats such as CSV, JSON, or any other format that suits the project requirements. The primary goal is to avoid hardcoding these threshold values directly in the coding scripts. This approach offers several benefits:

1. Easy browsing: Storing thresholds in separate files in this directory makes it simple to review and understand the thresholds used in a project.

2. Flexibility: Different file formats can be used based on the project's needs and the team's preferences.

3. Version control: Changes to thresholds can be easily tracked and managed using version control systems.

4. Maintainability: Updating thresholds becomes a straightforward process of modifying the files in this directory, rather than searching through code.

5. Reproducibility: Having a centralized location for thresholds ensures consistency across different parts of the project.

Examples of how thresholds might be stored:

1. CSV format (thresholds.csv):
   variable,lower_bound,upper_bound
   age,0,120
   heart_rate,40,200
   systolic_bp,70,220

2. JSON format (thresholds.json):
   {
     "age": {"min": 0, "max": 120},
     "heart_rate": {"min": 40, "max": 200},
     "systolic_bp": {"min": 70, "max": 220}
   }


Choose the format that best aligns with your project's needs and your team's workflow.

