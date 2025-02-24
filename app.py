import json
from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient
from datetime import datetime


app = Flask(__name__)

# Configure MongoDB connection
uri = "mongodb+srv://queryWizard:DatabaseSystems@cluster0.bdoz8u7.mongodb.net/"
client = MongoClient(uri)
db = client["HorrorMoviesDB"]
collection = db["DBCollection"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/runtime_language_analysis', methods=['POST'])
def runtime_language_analysis():
    # Group movies based on the five languages
    languages = ["en", "ja", "ru", "ko", "es"]

    # Define the runtime categories
    runtime_categories = {
        "short": {"$lt": 120},
        # "standard": {"$gte": 60, "$lte": 120},
        "long": {"$gt": 120}
    }

    # Prepare data structure to store average vote count for each language and category
    language_data = {lang: {category: 0 for category in runtime_categories} for lang in languages}

    for lang in languages:
        for category, runtime_query in runtime_categories.items():
            # Query to find movies in the specified language and runtime category
            query = {
                "original_language": lang,
                "runtime": runtime_query
            }

            # Aggregate query to calculate average vote count
            pipeline = [
                {"$match": query},
                {"$group": {"_id": None, "avg_vote_count": {"$avg": "$popularity"}}}
            ]

            # Execute the aggregate query
            result = collection.aggregate(pipeline)

            # Extract the average vote count or default to 0 if no result
            avg_vote_count = next(result, {"avg_vote_count": 0})["avg_vote_count"]

            # Store the average vote count in the data structure
            language_data[lang][category] = avg_vote_count

    print(f"language_data is {language_data}")
    
    return render_template('runtime_language_analysis.html', language_data=language_data)

@app.route('/month_profitTrend_analysis', methods=['POST'])
def month_profitTrend_analysis():
    # Aggregate query to calculate average budget, revenue, and profit for each month
    pipeline = [
        {
            "$project": {
                "_id": 0,
                "release_date": 1,
                "budget": 1,
                "revenue": 1,
                "profit": {"$subtract": ["$revenue", "$budget"]}
            }
        },
        {
            "$group": {
                "_id": {"month": {"$month": "$release_date"}},
                "avg_budget": {"$avg": "$budget"},
                "avg_revenue": {"$avg": "$revenue"},
                "avg_profit": {"$avg": "$profit"}
            }
        },
        {
            "$sort": {"_id.month": 1}
        }
    ]

    months_data = list(collection.aggregate(pipeline))

    print(f"months data is {months_data}\n\n")
    # Extract data for plotting
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    avg_profit_data = {month: 0 for month in months}
    print(f"avg profit before is {avg_profit_data}")
    for item in months_data:
        month = item["_id"]["month"]
        avg_profit = item["avg_profit"]

        avg_profit_data[months[int(month) - 1]] = avg_profit
    print(f"avg profit is {avg_profit_data}")
    return render_template('month_profitTrend_analysis.html', months=months, avg_profit_data=avg_profit_data)


@app.route('/popular_genre_analysis', methods=['POST'])
def popular_genre_analysis():
    # Genres to consider for analysis
    genres = [
        "Action", "Adventure", "Crime", "Fantasy", "Thriller", "Drama", "Mystery",
        "Science Fiction", "War", "Comedy", "Music", "Romance", "Documentary", "Animation", "Family", "Western"
    ]

    top_movies_by_category = {}
    avg_popularity_by_category = {}

    try:
        for genre in genres:
            query = {
                "genre_names": {"$regex": f"Horror.*{genre}|{genre}.*Horror"}
            }

            # Find top 1 movie based on popularity
            top_movie = collection.find(query).sort("popularity", -1).limit(1)
            top_movie_data = list(top_movie)

            if top_movie_data:
                top_movies_by_category[genre] = top_movie_data[0]["title"]
            else:
                top_movies_by_category[genre] = "N/A"

            # Calculate average popularity for the category
            avg_result = collection.aggregate([
                {"$match": query},
                {"$group": {"_id": None, "avg_popularity": {"$avg": "$popularity"}}}
            ])

            avg_popularity_by_category[genre] = next(avg_result, {"avg_popularity": 0})["avg_popularity"]

        # Convert dictionaries to JSON format
        # top_movies_by_category_json = json.dumps(top_movies_by_category)
        # avg_popularity_by_category_json = json.dumps(avg_popularity_by_category)

        print(f"Top movies by category: {top_movies_by_category}\n")
        print(f"Avg popularity by category: {avg_popularity_by_category}\n")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        top_movies_by_category_json = '{}'
        avg_popularity_by_category_json = '{}'

    return render_template(
        'popular_genre_analysis.html',
        top_movies_by_category=top_movies_by_category,
        avg_popularity_by_category=avg_popularity_by_category
    )

@app.route('/franchise_nonfranchise_analysis', methods=['POST'])
def franchise_nonfranchise_analysis():
    # Aggregate query to calculate average popularity for franchise and non-franchise movies for each decade
    pipeline = [
        {
            "$match": {
                "collection_name": {"$ne": None}  # Considering only non-null collection names
            }
        },
        {
            "$project": {
                "_id": 0,
                "collection_name": 1,
                "popularity": 1,
                "release_date": 1,
                "decade": {
                    "$concat": [
                        {"$substr": [{"$toString": {"$year": "$release_date"}}, 0, 3]},
                        "0s"
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {"collection_name": "$collection_name", "decade": "$decade"},
                "avg_popularity": {"$avg": "$popularity"}
            }
        },
        {
            "$sort": {"_id.decade": 1, "_id.collection_name": 1}
        }
    ]

    collections_data = list(collection.aggregate(pipeline))

    # Extract data for plotting
    decades = sorted(list(set(item["_id"]["decade"] for item in collections_data)))
    franchise_data = {decade: 0 for decade in decades}
    nonfranchise_data = {decade: 0 for decade in decades}

    for item in collections_data:
        collection_name = item["_id"]["collection_name"]
        decade = item["_id"]["decade"]
        avg_popularity = item["avg_popularity"]

        if collection_name.lower() == "na":
            nonfranchise_data[decade] = avg_popularity
        else:
            franchise_data[decade] = avg_popularity

    print(f"franchise_data is {franchise_data}")
    print(f"nonfranchise_data is {nonfranchise_data}")
    return render_template(
        'franchise_nonfranchise_analysis.html',
        decades=decades,
        franchise_data=franchise_data,
        nonfranchise_data=nonfranchise_data
    )


if __name__ == '__main__':
    app.run(debug=True)
