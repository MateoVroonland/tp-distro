import ast
import logging

def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)  
        return [data['name'] for data in dictionary_list]  
    except (ValueError, SyntaxError):
        return []

def process_movies_row(row):
    try:
        genres_idx = 3
        production_countries_idx = 13

        if len(row) > genres_idx:
            row[genres_idx] = str(dictionary_to_list(row[genres_idx]))
            
        if len(row) > production_countries_idx:
            row[production_countries_idx] = str(dictionary_to_list(row[production_countries_idx]))
            
        return row
    except Exception as e:
        logging.warning(f"Error processing movie row: {e}")
        return row
    
def process_credits_row(row):
    try:
        cast_idx = 1
        
        if len(row) > cast_idx:
            row[cast_idx] = str(dictionary_to_list(row[cast_idx]))
        
        return row
    except Exception as e:
        logging.warning(f"Error processing credits row: {e}")
        return row 