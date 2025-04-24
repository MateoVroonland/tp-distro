import ast
import logging
import json



def process_movies_row(row):

    try:
        genres_idx = 3
        production_countries_idx = 13

        if len(row) > genres_idx:
            genres = ast.literal_eval(row[genres_idx])
            row[genres_idx] = json.dumps(genres)
            
        if len(row) > production_countries_idx:
            countries = ast.literal_eval(row[production_countries_idx])
            row[production_countries_idx] = json.dumps(countries)
            
        return row
    except Exception as e:
        logging.warning(f"Error processing movie row: {e}")
        return row
    
def process_credits_row(row):
    try:
        cast_idx = 0
        
        if len(row) > cast_idx:
            cast = ast.literal_eval(row[cast_idx])
            row[cast_idx] = json.dumps(cast)
        
        return row
    except Exception as e:
        logging.warning(f"Error processing credits row: {e}")
        return row
  