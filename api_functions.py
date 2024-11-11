import pymysql


# MySQL database connection configuration
db_config = {
    "host": "127.0.0.1",
    "user": "flask_user",
    "password": "flask_user",
    "database": "flask_api_dev"
}
#Helper function to establish a database connection
def get_db_connection():
    """
    Establishes a connection to the MySQL database using the configuration in db_config.
    Returns a database connection object.
    """
    return pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        cursorclass=pymysql.cursors.DictCursor
    )

#Function to retrieve eaten dishes within a specified time range for a given user.
def internal_get_eaten_dishes(login, min_time, max_time):
    """
    Retrieves a list of dishes eaten by a user within a specified time range.
    Args:
        login (str): The user's login.
        min_time (datetime): The start of the time range.
        max_time (datetime): The end of the time range.
    Returns:
        tuple: A tuple containing a dictionary of results (or an error message) and an HTTP status code.
    """
    connection = get_db_connection()
    
    try:
        with connection.cursor() as cursor:
            # Retrieve user_id based on login
            cursor.execute("SELECT user_id FROM users WHERE login = %s", (login,))
            user = cursor.fetchone()
            if not user:
                return {"error": "Użytkownik nie istnieje"}, 404
            user_id = user["user_id"]
            # Query to fetch information about eaten meals within the specified time range
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

            # If the user has no data within this time range
            if not eaten_dishes:
                return {"message": "Brak danych o posiłkach w podanym okresie"}, 404

            # Transforming data into the appropriate format
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
