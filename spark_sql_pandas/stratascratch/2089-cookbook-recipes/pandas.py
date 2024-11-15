# Import your libraries
import pandas as pd

# Start writing code
cookbook_titles = pd.read_csv('cookbook_titles.csv')
max_page = cookbook_titles['page_number'].max()
page_with_even_index = pd.DataFrame([ i for i in range(0, max_page+1, 2)],
                                     columns=['page_number'])
page_with_even_index['temp'] = page_with_even_index['page_number'] + 1
page_with_even_index = page_with_even_index.merge(cookbook_titles, left_on='temp',
                                                   right_on='page_number', how='left')
page_with_even_index.rename(columns={'title':'right_title'}, inplace=True)
page_with_even_index.drop(columns=['temp','page_number_y'], inplace=True)
page_with_even_index = page_with_even_index.merge(cookbook_titles, 
                                                  left_on='page_number_x', 
                                                  right_on='page_number',
                                                    how='left')
page_with_even_index.drop(columns=['page_number'], inplace=True)
page_with_even_index.rename(columns={'title':'left_title',
                                     'page_number_x':'page_number'}, inplace=True)
page_with_even_index[['page_number','left_title', 'right_title']]