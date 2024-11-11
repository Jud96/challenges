# Import your libraries
import pandas as pd

# Start writing code
def find_students_with_median_writing_score(sat_scores):
    quantile = sat_scores['sat_writing'].quantile(0.5)
    return sat_scores[sat_scores['sat_writing'] == quantile][['student_id']]
