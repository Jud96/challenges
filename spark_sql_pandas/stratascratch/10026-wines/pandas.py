# Import your libraries
import pandas as pd

# Load your dataset
winemag_p1 = pd.read_csv('wines.csv', index_col=0)
# Start writing code
winemag_p1.head()
winemag_p1['description'] = winemag_p1['description'].str.lower().str.split('[,;\-\.\/ ]+')
winemag_p1[winemag_p1['description'].apply(lambda x: 'plum' in x or 'cherry' in x
                            or 'rose' in x or 'hazelnut' in x)][['winery']]