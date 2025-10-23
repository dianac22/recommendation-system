# Book recommendation system

To create the recommendation system, the following dataset was used:

Name: Goodreads Books  
Origin: Kaggle dataset by jealousleopard (jealousleopard/goodreadsbooks)  
Format: books.csv (comma-separated)

The CSV contained information about books, such as:

bookID – unique numeric identifier (string) - used as item_id in Recombee

title – book title (string)

authors – author names (comma-separated string)

average_rating – average Goodreads rating (float)

num_pages – number of pages (integer)

(also available: isbn, isbn13, language_code, ratings_count, text_reviews_count, publication_date, publisher)

This dataset was chosen because of its number of items, referencing meaningful attributes for recommendations. It also had a clean structure and consistent identifiers, which were easier to map to a recommendation catalog.

The project loads items (books) from the already mentioned Kaggle dataset and users (sales people) from a local CSV file, then pushes them to Recombee using the official SDK.
