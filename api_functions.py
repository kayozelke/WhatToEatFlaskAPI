import pymysql


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

def internal_get_eaten_dishes(login, min_time, max_time):
    connection = get_db_connection()
    
    try:
        with connection.cursor() as cursor:
            # Pobranie user_id na podstawie loginu
            cursor.execute("SELECT user_id FROM users WHERE login = %s", (login,))
            user = cursor.fetchone()
            if not user:
                return {"error": "Użytkownik nie istnieje"}, 404

            user_id = user["user_id"]

            # Zapytanie do bazy, aby pobrać informacje o spożytych posiłkach w podanym przedziale czasowym
                # SELECT ud.eat_time, ud.quantity, d.name
                # FROM user_past_dishes ud
                # JOIN dishes d ON ud.dishes_dish_id = d.dish_id
                # WHERE ud.users_user_id = %s AND ud.eat_time BETWEEN %s AND %s
                # ORDER BY ud.eat_time
            cursor.execute("""
                SELECT ud.eat_time, ud.quantity, d.name, t.rating
                FROM user_past_dishes ud
                JOIN dishes d ON ud.dishes_dish_id = d.dish_id
                LEFT JOIN user_tastes t ON t.dishes_dish_id = d.dish_id
                WHERE ud.users_user_id = %s AND ud.eat_time BETWEEN %s AND %s
                ORDER BY ud.eat_time
            """, (user_id, min_time, max_time))

            eaten_dishes = cursor.fetchall()

            # Jeśli użytkownik nie ma danych w tym przedziale czasowym
            if not eaten_dishes:
                return {"message": "Brak danych o posiłkach w podanym okresie"}, 404

            # Przekształcanie danych do odpowiedniego formatu
            dishes_info = [
                {
                    "eat_time": dish["eat_time"], 
                    "quantity": dish["quantity"], 
                    "dish_name": dish["name"],
                    "rating" : dish["rating"],
                } for dish in eaten_dishes
            ]
        return {"user": login, "eaten_dishes": dishes_info}, 200
    finally:
        connection.close()