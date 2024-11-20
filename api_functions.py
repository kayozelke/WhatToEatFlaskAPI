import pymysql
import time

# MySQL database connection configuration
db_config = {
    "host": "127.0.0.1",
    "user": "flask_user",
    "password": "flask_user",
    "database": "flask_api_dev"
}

MIN_USER_RATING = 3
MAX_AGE_HOURS = 24 * 28
SCORE_PER_HOUR = 1

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

def internal_get_user_id(login):
    """
    Retrieves the user ID associated with a given login.
    Args:
        login (str): The user's login.
    Returns:
        int: The user ID, or None if the user is not found.
    """
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT user_id FROM users WHERE login = %s", (login,))
            user = cursor.fetchone()
            if user:
                return user["user_id"]
            else:
                return None
    finally:
        connection.close()


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
                
            # SELECT 
            #     t.*, 
            #     last_eat.*
            # FROM user_tastes t
            # LEFT JOIN (
            #     SELECT dishes_dish_id, MAX(eat_time) AS last_eat_time, quantity
            #     FROM user_past_dishes
            #     WHERE users_user_id = 2
            #     GROUP BY dishes_dish_id
            # ) last_eat ON t.dishes_dish_id = last_eat.dishes_dish_id
            # WHERE t.users_user_id = 2 AND t.rating >= 3;
                
                
            cursor.execute("""
                SELECT ud.eat_time, ud.quantity, ud.dishes_dish_id, d.name, t.rating
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
                    "dish_id": dish["dishes_dish_id"],
                    "rating" : dish["rating"],
                } for dish in eaten_dishes
            ]
        return {"user": login, "eaten_dishes": dishes_info}, 200
    finally:
        connection.close()

def internal_get_user_dishes_ratings(user_id, min_rating = 0):
    """Returns a dictionary of user dishes ratings.
    Args:
        login (_type_): _description_
    """
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT t.rating, t.dishes_dish_id FROM user_tastes t WHERE t.users_user_id = {user_id} and t.rating >= {min_rating}
            """
            cursor.execute(query=query)
            user_ratings = cursor.fetchall()
            
            ratings_dict = {dish["dishes_dish_id"]: dish["rating"] for dish in user_ratings}
            return ratings_dict
    finally:
        connection.close()

def internal_get_user_dishes_scores(user_id, min_time = int(time.time()) - MAX_AGE_HOURS*3600, max_time = int(time.time())):
    
    user_dishes_scores = internal_get_user_dishes_ratings(user_id, MIN_USER_RATING)
    
    if not user_dishes_scores:
        return {"message": "Brak informacji o upodobaniach użytkownika"}, 404
    
    else: print(user_dishes_scores)
    
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            
            
            
            sql_query = f"""
                SELECT ud.dishes_dish_id, ud.eat_time, ud.quantity
                FROM user_tastes t 
                JOIN user_past_dishes ud ON ud.dishes_dish_id = t.dishes_dish_id  
                WHERE t.users_user_id = {user_id} AND t.rating >= {MIN_USER_RATING} AND ud.eat_time BETWEEN {min_time} AND {max_time};
            """
            print(sql_query)
            cursor.execute(sql_query)
            
            
            
            user_past_dishes = cursor.fetchall()
            
            for key,value in user_dishes_scores.items():
                # score calculations
                total_score = 0
                for dish_data in user_past_dishes:
                    if dish_data.get("dishes_dish_id") != key: continue
                    score = SCORE_PER_HOUR * round((dish_data['eat_time'] - min_time)/3600) * dish_data["quantity"]
                    
                    print(f"Dish ID: {key}, Added score: {score}, Eat Time (unix): {dish_data['eat_time']}, Eat Time (datetime): {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dish_data['eat_time']))}, Min Time (unix): {min_time}, Min Time (datetime): {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(min_time))}, Quantity: {dish_data['quantity']}")

                    total_score += score
                user_dishes_scores[key] = {
                    "rating" : value,
                    "score" : total_score
                }
            return user_dishes_scores, 200
            
    finally:
        connection.close()

def internal_get_user_dishes_scores_debug1(user_id, min_time = int(time.time()) - MAX_AGE_HOURS*3600, max_time = int(time.time())):
    
    user_dishes_scores = internal_get_user_dishes_ratings(user_id, MIN_USER_RATING)
    
    if not user_dishes_scores:
        return {"message": "Brak informacji o upodobaniach użytkownika"}, 404
    
    else: print(user_dishes_scores)
    
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            
            
            
            sql_query = f"""
                SELECT ud.dishes_dish_id, d.name, ud.eat_time, ud.quantity
                FROM user_tastes t 
                JOIN user_past_dishes ud ON ud.dishes_dish_id = t.dishes_dish_id  
                JOIN dishes d ON ud.dishes_dish_id = d.dish_id
                WHERE t.users_user_id = {user_id} AND t.rating >= {MIN_USER_RATING} AND ud.eat_time BETWEEN {min_time} AND {max_time};
            """
            print(sql_query)
            cursor.execute(sql_query)
            
            
            
            user_past_dishes = cursor.fetchall()
            
            for key,value in user_dishes_scores.items():
                # score calculations
                total_score = 0
                for dish_data in user_past_dishes:
                    if dish_data.get("dishes_dish_id") != key: continue
                    score = SCORE_PER_HOUR * round((dish_data['eat_time'] - min_time)/3600) * dish_data["quantity"]
                    
                    print(f"Added score for id {dish_data['dishes_dish_id']}: {score}, Eat Time (unix): {dish_data['eat_time']}, as dt: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dish_data['eat_time']))}, Min Time (unix): {min_time}, as dt: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(min_time))}, Quantity: {dish_data['quantity']}, Dish name: {dish_data['name']}, ")

                    total_score += score
                user_dishes_scores[key] = {
                    "rating" : value,
                    "score" : total_score
                }
            return user_dishes_scores, 200
            
    finally:
        connection.close()