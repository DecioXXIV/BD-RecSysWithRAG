import numpy as np
import pandas as pd

class InfRetriever:
    def __init__(self, userId):
        self.userId = userId
        self.chosenMovieIds = None

    def computeUserPreferences(self, sample_size):
        ratings = pd.read_csv("./datasets/ratings.csv")
        userPrefs = ratings[(ratings['userId'] == self.userId) & (ratings['rating'] >= 4.5)]
        userMovieIds = userPrefs['movieId'].values

        if sample_size > userPrefs.shape[0]:
            sample_size = int(0.67* userPrefs.shape[0])

        probs =  [1/userMovieIds.shape[0]] * userMovieIds.shape[0]
        chosenMovieIds = np.random.choice(userMovieIds, size=sample_size, replace=False, p=probs)

        self.chosenMovieIds = chosenMovieIds
    
    def getMovieTitles(self):
        movies = pd.read_csv("./datasets/movies.csv")
        titles = list()

        for movieId in self.chosenMovieIds:
            title = movies[movies['movieId'] == movieId]['title'].values[0]
            titles.append(title)
        
        return titles

    def getMovieDetailsDF(self):
        df = pd.DataFrame(columns=['title', 'director', 'genres', 'plot', 'year', 'cast'], index=self.chosenMovieIds)

        for movieId in self.chosenMovieIds:
            txt_file = open(f"./movie_text_data/movie_{movieId}.txt", "r")
            lines = txt_file.readlines()

            title = lines[1]
            title = title.replace("TITLE -> ", "")
            title = title.replace("\n", "")
            df.at[movieId, 'title'] = title

            director = lines[2]
            director = director.replace("DIRECTED BY -> ", "")
            director = director.replace("\n", "")
            df.at[movieId, 'director'] = director

            genres = lines[3]
            genres = genres.replace("GENRES -> ", "")
            genres = genres.replace("\n", "")
            df.at[movieId, 'genres'] = genres

            plot = lines[4]
            plot = plot.replace("PLOT -> ", "")
            genres = plot.replace("\n", "")
            df.at[movieId, 'plot'] = plot

            year = lines[5]
            year = year.replace("YEAR -> ", "")
            year = year.replace("\n", "")
            df.at[movieId, 'year'] = year

            cast = lines[6]
            cast = cast.replace("STARS -> ", "")
            cast = cast.replace("\n", "")
            df.at[movieId, 'cast'] = cast
        
        return df