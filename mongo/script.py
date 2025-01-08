from pymongo import MongoClient, InsertOne
from tqdm import tqdm
import time
import uuid
import random


def connect_to_db():
    client = MongoClient("mongodb://root:password@mongodb:27017/")
    return client["test_db"]


def create_collections(db):
    db.users.drop()
    db.films.drop()
    db.likes.drop()
    db.bookmarks.drop()

    db.likes.create_index([("user_id", 1), ("rating", 1)])
    db.likes.create_index([("film_id", 1), ("rating", 1)])
    db.bookmarks.create_index("user_id")
    db.bookmarks.create_index("film_id")
    db.films.create_index("_id")
    db.users.create_index("_id")


def generate_data():
    users = [{"_id": str(uuid.uuid4()), "name": f"User {i}", "email": f"user{i}@example.com"} for i in range(2000000)]
    films = [{"_id": str(uuid.uuid4()), "title": f"Film {i}", "genre": random.choice(["Action", "Drama"])} for i in range(300000)]
    likes = [{"user_id": random.choice(users)["_id"], "film_id": random.choice(films)["_id"], "rating": random.randint(0, 10)} for _ in range(5000000)]
    bookmarks = [{"user_id": random.choice(users)["_id"], "film_id": random.choice(films)["_id"]} for _ in range(2700000)]
    return users, films, likes, bookmarks


def insert_data(db, users, films, likes, bookmarks, batch_size=10000):
    total_records = len(users) + len(films) + len(likes) + len(bookmarks)
    progress_bar = tqdm(total=total_records, desc="Inserting data", unit="records")

    def insert_with_batches(collection, data):
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            collection.bulk_write([InsertOne(doc) for doc in batch], ordered=False)
            progress_bar.update(len(batch))

    start_time = time.time()

    insert_with_batches(db.users, users)
    insert_with_batches(db.films, films)
    insert_with_batches(db.likes, likes)
    insert_with_batches(db.bookmarks, bookmarks)

    end_time = time.time()
    progress_bar.close()

    print(f"Вставка данных: {end_time - start_time:.2f} секунд")


def real_time_data_test(db, num_trials=100):
    progress_bar = tqdm(total=num_trials, desc="Real-time data test", unit="trials")
    issues_detected = 0

    for _ in range(num_trials):
        film_id = random.choice(list(db.films.find({}, {"_id": 1})))["_id"]

        # Считаем текущее количество лайков
        initial_likes_count = db.likes.count_documents({"film_id": film_id, "rating": {"$gt": 5}})

        # Добавляем лайк
        db.likes.insert_one({
            "user_id": str(uuid.uuid4()),
            "film_id": film_id,
            "rating": random.randint(6, 10)
        })

        # Проверяем количество лайков
        updated_likes_count = db.likes.count_documents({"film_id": film_id, "rating": {"$gt": 5}})

        # Проверяем, что счетчик обновился
        if updated_likes_count != initial_likes_count + 1:
            issues_detected += 1

        progress_bar.update(1)

    progress_bar.close()

    if issues_detected > 0:
        print(f"Обнаружены проблемы с моментальным отображением в {issues_detected} случаях.")
    else:
        print("Все операции успешно отображаются без задержек.")


def query_data(db, users, films, num_trials=100):
    liked_movies_times = []
    likes_count_times = []
    bookmarks_times = []
    avg_rating_times = []

    for _ in range(num_trials):
        # 1. Список понравившихся фильмов пользователя
        user_id = random.choice(users)["_id"]
        start_time = time.time()
        list(db.likes.aggregate([
            {"$match": {"user_id": user_id, "rating": {"$gt": 5}}},
            {"$lookup": {
                "from": "films",
                "localField": "film_id",
                "foreignField": "_id",
                "as": "liked_films"
            }},
            {"$unwind": "$liked_films"},
            {"$project": {"title": "$liked_films.title"}}
        ]))
        liked_movies_times.append(time.time() - start_time)

        # 2. Количество лайков у фильма
        film_id = random.choice(films)["_id"]
        start_time = time.time()
        db.likes.count_documents({"film_id": film_id, "rating": {"$gt": 5}})
        likes_count_times.append(time.time() - start_time)

        # 3. Список закладок
        start_time = time.time()
        list(db.bookmarks.aggregate([
            {"$match": {"user_id": user_id}},
            {"$lookup": {
                "from": "films",
                "localField": "film_id",
                "foreignField": "_id",
                "as": "bookmarked_films"
            }},
            {"$unwind": "$bookmarked_films"},
            {"$project": {"title": "$bookmarked_films.title"}}
        ]))
        bookmarks_times.append(time.time() - start_time)

        # 4. Средняя пользовательская оценка фильма
        start_time = time.time()
        list(db.likes.aggregate([
            {"$match": {"film_id": film_id}},
            {"$group": {"_id": None, "average_rating": {"$avg": "$rating"}}}
        ]))
        avg_rating_times.append(time.time() - start_time)

    print(f"Среднее время запроса 'Список понравившихся фильмов': {sum(liked_movies_times) / num_trials:.4f} секунд")
    print(f"Среднее время запроса 'Количество лайков у фильма': {sum(likes_count_times) / num_trials:.4f} секунд")
    print(f"Среднее время запроса 'Список закладок': {sum(bookmarks_times) / num_trials:.4f} секунд")
    print(f"Среднее время запроса 'Средняя оценка фильма': {sum(avg_rating_times) / num_trials:.4f} секунд")


if __name__ == "__main__":
    db = connect_to_db()
    create_collections(db)
    users_data, films_data, likes_data, bookmarks_data = generate_data()
    insert_data(db, users_data, films_data, likes_data, bookmarks_data)
    query_data(db, users_data, films_data)
    real_time_data_test(db)
