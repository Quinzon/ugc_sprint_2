from tqdm import tqdm
import psycopg2
import time
import uuid
import random


def connect_to_db():
    return psycopg2.connect(
        dbname="test_db",
        user="user",
        password="password",
        host="postgres",
        port="5432",
    )


def create_tables():
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute("""
    DROP TABLE IF EXISTS users CASCADE;
    DROP TABLE IF EXISTS films CASCADE;
    DROP TABLE IF EXISTS likes CASCADE;
    DROP TABLE IF EXISTS bookmarks CASCADE;

    CREATE TABLE users (
        id UUID PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255) UNIQUE
    );

    CREATE TABLE films (
        id UUID PRIMARY KEY,
        title VARCHAR(255),
        genre VARCHAR(255)
    );

    CREATE TABLE likes (
        user_id UUID REFERENCES users(id),
        film_id UUID REFERENCES films(id),
        rating SMALLINT CHECK (rating BETWEEN 0 AND 10),
        PRIMARY KEY (user_id, film_id)
    );

    CREATE TABLE bookmarks (
        user_id UUID REFERENCES users(id),
        film_id UUID REFERENCES films(id),
        PRIMARY KEY (user_id, film_id)
    );
    """)
    connection.commit()
    cursor.close()
    connection.close()


def generate_data():
    users = [(str(uuid.uuid4()), f"User {i}", f"user{i}@example.com") for i in range(2000000)]
    films = [(str(uuid.uuid4()), f"Film {i}", random.choice(["Action", "Drama"])) for i in range(300000)]
    likes = [(random.choice(users)[0], random.choice(films)[0], random.randint(0, 10)) for _ in range(5000000)]
    bookmarks = [(random.choice(users)[0], random.choice(films)[0]) for _ in range(2700000)]
    return users, films, likes, bookmarks


def insert_data(users, films, likes, bookmarks, batch_size=10000):
    connection = connect_to_db()
    cursor = connection.cursor()

    total_records = len(users) + len(films) + len(likes) + len(bookmarks)
    progress_bar = tqdm(total=total_records, desc="Inserting data", unit="records")

    def insert_with_batches(query, data):
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(query, batch)
            connection.commit()
            progress_bar.update(len(batch))

    start_time = time.time()

    insert_with_batches("""
        INSERT INTO users (id, name, email) 
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name, email = EXCLUDED.email
    """, users)

    insert_with_batches("""
        INSERT INTO films (id, title, genre) 
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET title = EXCLUDED.title, genre = EXCLUDED.genre
    """, films)

    insert_with_batches("""
        INSERT INTO likes (user_id, film_id, rating) 
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, film_id) DO UPDATE
        SET rating = EXCLUDED.rating
    """, likes)

    insert_with_batches("""
        INSERT INTO bookmarks (user_id, film_id) 
        VALUES (%s, %s)
        ON CONFLICT (user_id, film_id) DO NOTHING
    """, bookmarks)

    end_time = time.time()
    progress_bar.close()

    cursor.close()
    connection.close()
    print(f"Вставка данных: {end_time - start_time:.2f} секунд")


def query_data(users, films, num_trials=100):
    connection = connect_to_db()
    cursor = connection.cursor()

    liked_movies_times = []
    likes_count_times = []
    bookmarks_times = []
    avg_rating_times = []

    for _ in range(num_trials):
        # 1. Список понравившихся фильмов пользователя
        user_id = random.choice(users)[0]
        start_time = time.time()
        cursor.execute("""
            SELECT f.title FROM likes l 
            JOIN films f ON l.film_id = f.id 
            WHERE l.user_id = %s AND l.rating > 5""", (user_id,))
        liked_movies_times.append(time.time() - start_time)

        # 2. Количество лайков у фильма
        film_id = random.choice(films)[0]
        start_time = time.time()
        cursor.execute("SELECT COUNT(*) FROM likes WHERE film_id = %s AND rating > 5", (film_id,))
        likes_count_times.append(time.time() - start_time)

        # 3. Список закладок
        start_time = time.time()
        cursor.execute("""
            SELECT f.title FROM bookmarks b
            JOIN films f ON b.film_id = f.id
            WHERE b.user_id = %s""", (user_id,))
        bookmarks_times.append(time.time() - start_time)

        # 4. Средняя пользовательская оценка фильма
        start_time = time.time()
        cursor.execute("""
            SELECT AVG(rating) FROM likes WHERE film_id = %s""", (film_id,))
        avg_rating_times.append(time.time() - start_time)

    cursor.close()
    connection.close()

    print(f"Среднее время запроса 'Список понравившихся фильмов': {sum(liked_movies_times) / num_trials:.4f} секунд")
    print(f"Среднее время запроса 'Количество лайков у фильма': {sum(likes_count_times) / num_trials:.4f} секунд")
    print(f"Среднее время запроса 'Список закладок': {sum(bookmarks_times) / num_trials:.4f} секунд")
    print(f"Среднее время запроса 'Средняя оценка фильма': {sum(avg_rating_times) / num_trials:.4f} секунд")


if __name__ == "__main__":
    create_tables()
    users_data, films_data, likes_data, bookmarks_data = generate_data()
    insert_data(users_data, films_data, likes_data, bookmarks_data)
    query_data(users_data, films_data)
