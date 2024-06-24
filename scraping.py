import os, time, requests
import pandas as pd
from lxml import html
from concurrent.futures import ThreadPoolExecutor

HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
         "Accept-Language": "en-US,en;q=0.5"}
CWD = os.getcwd()
NUM_WORKERS = int(os.cpu_count() / 2)

def scrape_movie_data(num, movie):
    if not os.path.exists(CWD + f"/movie_text_data/movie_{num}.txt"):
        full_retrieval = False
        title, director, genres, brief_plot, year, stars = None, None, None, None, None, None
        inner_dict = dict()
        condition, iter = 0, 0
        url = "https://www.imdb.com/title/tt" + str(movie) + "/"

        while full_retrieval is False and iter < 5:
            page = requests.get(url, headers=HEADERS)

            tree = html.fromstring(page.content)

            if title == None or title == []:
                title = tree.xpath("//span[@data-testid='hero__primary-text']/text()")[0]
                if title != None and title != []:
                    inner_dict['title'] = title
                    condition += 1

            if director == None or director == []:
                director = tree.xpath("//a[@class='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link']/text()")[0]
                if director != None and director != []:
                    inner_dict['director'] = director
                    condition += 1

            if genres == None or genres == []:
                genres = tree.xpath("//span[@class='ipc-chip__text']/text()")[:-1]
                if genres != None and genres != []:
                    inner_dict['genres'] = genres
                    condition += 1

            if brief_plot == None or brief_plot == []:
                brief_plot = tree.xpath("//span[@data-testid='plot-xs_to_m']/text()")[0]
                if brief_plot != None and brief_plot != []:
                    inner_dict['brief_plot'] = brief_plot
                    condition += 1

            if year == None or year == []:
                year = tree.xpath("//div[@class='sc-b7c53eda-0 dUpRPQ']//*/a[@class='ipc-link ipc-link--baseAlt ipc-link--inherit-color']/text()")[0]
                if year != None and year != []:
                    inner_dict['year'] = year
                    condition += 1

            if stars == None or stars == []:
                stars = tree.xpath("//div[@class='sc-b7c53eda-2 iOESUA']/div/ul[@class='ipc-metadata-list ipc-metadata-list--dividers-all title-pc-list ipc-metadata-list--baseAlt']/li[@class='ipc-metadata-list__item ipc-metadata-list-item--link'][last()]//*/a[@class='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link']/text()")
                if stars != None and stars != []:
                    inner_dict['stars'] = stars
                    condition += 1
        
            if condition == 6:
                full_retrieval = True
                file_name = CWD + f"/movie_text_data/movie_{num}.txt"
                with open(file_name, 'w') as f:
                    f.write(f"IMDB ID -> {movie}\n")
                    f.write(f"TITLE -> {inner_dict['title']}\n")
                    f.write(f"DIRECTED BY -> {inner_dict['director']}\n")
                    f.write(f"GENRES -> {', '.join(inner_dict['genres'])}\n")
                    f.write(f"PLOT -> {inner_dict['brief_plot']}\n")
                    f.write(f"YEAR -> {inner_dict['year']}\n")
                    f.write(f"STARS -> {', '.join(inner_dict['stars'])}\n")

            time.sleep(0.25)
            iter += 1
        print(f"Number {num}, MOVIE -> {url}")


### #### ###
### MAIN ###
### #### ###
if not os.path.exists(CWD + "/movie_text_data"):
    os.mkdir(CWD + "/movie_text_data")

movies = pd.read_csv(CWD + "/datasets/links.csv", dtype=str)
movie_ids = movies["imdbId"].tolist()

print("Begin Scraping...")
with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    executor.map(scrape_movie_data, range(len(movie_ids)), movie_ids)