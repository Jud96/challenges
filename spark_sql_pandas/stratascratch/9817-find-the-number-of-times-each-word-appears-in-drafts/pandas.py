# Import your libraries
import pandas as pd

# Start writing code
google_file_store = pd.read_csv("find-the-number-of-times-each-word-appears-in-drafts/data.csv")
drafts = google_file_store[google_file_store['filename'].str.contains('draft')]
result = drafts['contents'].str.lower().str.split(pat='\W+')\
    .explode().value_counts().reset_index()
result = result[result['index'] != '']
