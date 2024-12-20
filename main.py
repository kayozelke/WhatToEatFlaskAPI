import time
from flask import Flask, request, jsonify
import pymysql
import api_functions as af

app = Flask(__name__)

# Konfiguracja połączenia z bazą danych MySQL
db_config = {
    "host": "127.0.0.1",
    "user": "flask_user",
    "password": "flask_user",
    "database": "flask_api_dev"
}

# Funkcja pomocnicza do połączenia z bazą danych
def get_db_connection():
    return pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        cursorclass=pymysql.cursors.DictCursor
    )
    

@app.route('/')
def index():
    # return af.get_user_dishes_ratings(1)
    return af.internal_get_user_dishes_scores(request.args.get("id", 1, type=int))
    # return '<h1>Hello!</h1>'

# Endpoint do dodawania informacji o tym, co użytkownik zjadł
@app.route("/add-eaten-dish", methods=["POST"])
def add_eaten_dish():
    data = request.get_json()
    login = data.get("login")
    dish_id = data.get("dish_id")
    eat_time = data.get("eat_time")
    quantity = data.get("quantity")

    # Sprawdzanie, czy wszystkie dane są dostarczone
    if not all([login, dish_id, eat_time, quantity]):
        return jsonify({"error": "Wszystkie pola są wymagane"}), 400

    # Połączenie z bazą danych
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Pobranie user_id na podstawie loginu
            cursor.execute("SELECT user_id FROM users WHERE login = %s", (login,))
            user = cursor.fetchone()
            if not user:
                return jsonify({"error": "Użytkownik nie istnieje"}), 404

            user_id = user["user_id"]

            # Pobranie maksymalnego user_past_dish_id
            cursor.execute("SELECT MAX(user_past_dish_id) as max_user_past_dish_id FROM user_past_dishes")
            result = cursor.fetchone()

            # Jeśli wynik jest None, ustawiamy max_user_past_dish_id na 0
            max_user_past_dish_id = 0 if result.get("max_user_past_dish_id", None) is None else result["max_user_past_dish_id"]
            new_user_past_dish_id = max_user_past_dish_id + 1

            # Wstawianie nowego rekordu do tabeli user_past_dishes
            cursor.execute(
                "INSERT INTO user_past_dishes (user_past_dish_id, eat_time, quantity, users_user_id, dishes_dish_id) "
                "VALUES (%s, %s, %s, %s, %s)",
                (new_user_past_dish_id, eat_time, quantity, user_id, dish_id)
            )
            connection.commit()

        return jsonify({"message": "Danie zostało dodane pomyślnie"}), 201
    finally:
        connection.close()


@app.route("/add-user-taste", methods=["POST"])
def add_user_taste():
    data = request.get_json()
    login = data.get("login")
    dish_id = data.get("dish_id")
    rating = data.get("rating")

    # Sprawdzanie, czy wszystkie dane są dostarczone
    if not all([login, dish_id, rating]):
        return jsonify({"error": "Wszystkie pola są wymagane"}), 400

    # Połączenie z bazą danych
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Pobranie user_id na podstawie loginu
            cursor.execute("SELECT user_id FROM users WHERE login = %s", (login,))
            user = cursor.fetchone()
            if not user:
                return jsonify({"error": "Użytkownik nie istnieje"}), 404

            user_id = user["user_id"]

            # Sprawdzenie, czy ocena dla tego użytkownika i dania już istnieje
            cursor.execute(
                "SELECT taste_id FROM user_tastes WHERE users_user_id = %s AND dishes_dish_id = %s",
                (user_id, dish_id)
            )
            existing_taste = cursor.fetchone()

            if existing_taste:
                # Jeśli ocena już istnieje, zaktualizuj ją
                cursor.execute(
                    "UPDATE user_tastes SET rating = %s WHERE taste_id = %s",
                    (rating, existing_taste["taste_id"])
                )
                message = "Ocena została zaktualizowana pomyślnie"
            else:
                # Jeśli ocena nie istnieje, nadaj nowy taste_id i wstaw nowy rekord
                cursor.execute("SELECT MAX(taste_id) as max_taste_id FROM user_tastes")
                result = cursor.fetchone()
                
                # print(f"--- {type(result)} --- {result}")
                # Jeśli wynik jest None, ustawiamy max_taste_id na 0
                max_taste_id = 0 if result.get("max_taste_id", None) is None else result["max_taste_id"]
                new_taste_id = max_taste_id + 1
                # print(f"new_taste_id: {new_taste_id}")
                # exit()
                cursor.execute(
                    "INSERT INTO user_tastes (taste_id, rating, users_user_id, dishes_dish_id) VALUES (%s, %s, %s, %s)",
                    (new_taste_id, rating, user_id, dish_id)
                )
                message = "Ocena została dodana pomyślnie"

            connection.commit()
        
        return jsonify({"message": message}), 201

    finally:
        connection.close()

# Endpoint do wyświetlania spożytych posiłków w podanym przedziale czasowym
@app.route("/get-eaten-dishes", methods=["GET"])
def get_eaten_dishes():
    # Pobieramy login, min_time i max_time z parametrów zapytania
    login = request.args.get("login")
    min_time = request.args.get("min_time", default=0, type=int)
    max_time = request.args.get("max_time", default=int(time.time()+24*3600), type=int)
    
    # print(f"login: {login}, min_time: {min_time}, max_time: {max_time}")
    
    # Sprawdzanie, czy wszystkie dane są dostarczone
    if not login or min_time is None or max_time is None:
        return jsonify({"error": "Wszystkie parametry są wymagane"}), 400

    result, code = af.internal_get_eaten_dishes(login, min_time, max_time)
    
    return jsonify(result), code

@app.route("/get-user-dishes-scores", methods=["POST"])
def get_user_dishes_scores():
    data = request.get_json()
    login = data.get("login")
    # print(login)
    if not login:
        return jsonify({"error": "Wszystkie parametry są wymagane"}), 400
    
    user_id = af.internal_get_user_id(login)

    # result, code = af.internal_get_user_dishes_scores(user_id)
    result, code = af.internal_get_user_dishes_scores_debug1(user_id)
    # result, code = af.internal_get_user_dishes_scores(user_id, int(time.time()) - 24*27*3600, int(time.time()+24*3600))
    
    return jsonify(result), code

if __name__ == "__main__":
    app.run(debug=False)
