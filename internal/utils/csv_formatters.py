import pandas as pd
import ast

def dictionary_to_list(dictionary_str):
        try:
            dictionary_list = ast.literal_eval(dictionary_str)  
            return [data['name'] for data in dictionary_list]  
        except (ValueError, SyntaxError):
            return [] 

def clean_movies_csv(file_path):
    movies_df_columns = ["id", "title", "genres", "release_date", "overview", "production_countries", "spoken_languages", "budget", "revenue"]
    movies_df = pd.read_csv(file_path)
    
    movies_df_cleaned = movies_df.dropna(subset=movies_df_columns)[movies_df_columns].copy()
    
    movies_df_cleaned['release_date'] = pd.to_datetime(movies_df_cleaned['release_date'], format='%Y-%m-%d', errors='coerce')
    
    movies_df_cleaned['budget'] = pd.to_numeric(movies_df_cleaned['budget'], errors='coerce')
    movies_df_cleaned['revenue'] = pd.to_numeric(movies_df_cleaned['revenue'], errors='coerce')

    movies_df_cleaned['genres'] = movies_df_cleaned['genres'].apply(dictionary_to_list)
    movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].apply(dictionary_to_list)
    movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].apply(dictionary_to_list)
    
    movies_df_cleaned['genres'] = movies_df_cleaned['genres'].astype(str)
    movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].astype(str)
    movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].astype(str)
    
    return movies_df_cleaned

def clean_ratings_csv(file_path):
    ratings_df_columns = ["movieId", "rating", "timestamp"]
    ratings_df = pd.read_csv(file_path)
    
    ratings_df_cleaned = ratings_df.dropna(subset=ratings_df_columns)[ratings_df_columns].copy()
    ratings_df_cleaned['timestamp'] = pd.to_datetime(ratings_df_cleaned['timestamp'], unit='s')
    
    return ratings_df_cleaned

def clean_credits_csv(file_path):
    credits_df_columns = ["id", "cast"]
    credits_df = pd.read_csv(file_path)
    
    credits_df_cleaned = credits_df.dropna(subset=credits_df_columns)[credits_df_columns].copy()
    
    credits_df_cleaned['cast'] = credits_df_cleaned['cast'].apply(dictionary_to_list)
    credits_df_cleaned['cast'] = credits_df_cleaned['cast'].astype(str)
    
    return credits_df_cleaned