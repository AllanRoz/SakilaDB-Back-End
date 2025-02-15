from flask import Flask, jsonify, request
from flask_cors import CORS
import pymysql

app = Flask(__name__)
cors = CORS(app, origins='*')

# Configuring MySQL Database connection
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'sakila'

def get_db():
    return pymysql.connect(host=app.config['MYSQL_HOST'],
                            user=app.config['MYSQL_USER'],
                            password=app.config['MYSQL_PASSWORD'],
                            db=app.config['MYSQL_DB'],
                            cursorclass=pymysql.cursors.DictCursor)

@app.route('/', methods=['GET'])
def index():
    return "home"


# Landing Page Feature 1
# As a user I want to view top 5 rented films of all times
@app.route("/top/rented_films", methods=['GET'])
def top5films():
    db = get_db()
    cursor = db.cursor()
    sql_query = """SELECT f.film_id, f.title, c.name AS category_name, 
                    COUNT(r.rental_id) AS rented FROM sakila.film f 
                    JOIN sakila.inventory i ON f.film_id = i.film_id 
                    JOIN sakila.rental r ON i.inventory_id = r.inventory_id 
                    JOIN sakila.film_category fc ON f.film_id = fc.film_id 
                    JOIN sakila.category c ON fc.category_id = c.category_id 
                    GROUP BY f.film_id, f.title, c.name 
                    ORDER BY rented DESC LIMIT 5;"""
    cursor.execute(sql_query)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

# Landing Page Feature 2
# As a user I want to be able to click on any of the top 5 films and view its details
@app.route("/details/film", methods=['POST'])
def film_details():
    db = get_db()
    cursor = db.cursor()
    
    # Getting data film_id
    data = request.get_json()
    film_id = data['film_id']

    sql_query = """SELECT * FROM sakila.film WHERE film_id = %s;"""
    cursor.execute(sql_query, (film_id))
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)


# Landing Page Feature 3
# As a user I want to be able to view top 5 actors that are part of films I have in the store
@app.route("/top/actors", methods=['GET'])
def top5actors():
    db = get_db()
    cursor = db.cursor()
    sql_query = """SELECT actor.actor_id, actor.first_name, actor.last_name, 
                    COUNT(film_actor.film_id) AS movies 
                    FROM sakila.actor 
                    JOIN sakila.film_actor on actor.actor_id = film_actor.actor_id 
                    GROUP BY actor.actor_id 
                    ORDER BY movies DESC LIMIT 5;"""
    cursor.execute(sql_query)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

# Landing Page Feature 4
# As a user I want to be able to view the actor’s details and view their top 5 rented films
# @app.route("/details/actor", methods=['POST'])
# def actor_details():
#     db = get_db()
#     cursor = db.cursor()
    
#     # Getting data film_id
#     data = request.get_json()
#     actor_id = data['actor_id']

#     sql_query = """SELECT * FROM sakila.actor WHERE actor_id = %s;"""
#     cursor.execute(sql_query, (actor_id))
#     results = cursor.fetchall()
#     cursor.close()
#     db.close()
#     return jsonify(results)
@app.route("/details/actor", methods=['POST'])
def actor_details():
    db = get_db()
    cursor = db.cursor()
    
    # Getting data film_id
    data = request.get_json()
    actor_id = data['actor_id']
    sql_query = """
                WITH actor_details AS (
                    SELECT actor_id, first_name, last_name, last_update
                    FROM sakila.actor
                    WHERE actor_id = %s
                ),
                top_rented_movies AS (
                    SELECT f.film_id, f.title, COUNT(r.rental_id) AS rental_count
                    FROM sakila.film f
                    JOIN sakila.inventory i ON f.film_id = i.film_id
                    JOIN sakila.rental r ON i.inventory_id = r.inventory_id
                    JOIN sakila.film_actor fa ON f.film_id = fa.film_id
                    WHERE fa.actor_id = %s
                    GROUP BY f.film_id, f.title
                    ORDER BY rental_count DESC
                    LIMIT 5
                )
                SELECT 
                    a.actor_id, a.first_name, a.last_name, a.last_update,
                    m.film_id, m.title, m.rental_count
                FROM actor_details a
                LEFT JOIN top_rented_movies m ON 1=1;
                """
    cursor.execute(sql_query, (actor_id, actor_id))
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)


# @app.route("/top_actor_films", methods=['POST'])
# def top_films_of_top_actors():
#     db = get_db()
#     cursor = db.cursor()

#     # Getting data film_id
#     data = request.get_json()
#     actor_id = data['actor_id']

#     sql_query = """WITH actor_most AS (
#                         SELECT a.actor_id, a.first_name, a.last_name, 
#                         COUNT(fa.film_id) AS film_count
#                         FROM sakila.actor a
#                         JOIN sakila.film_actor fa ON a.actor_id = fa.actor_id
#                         WHERE a.actor_id = %s
#                         GROUP BY a.actor_id, a.first_name, a.last_name
#                     ),
#                     top_rented_movies AS (
#                         SELECT f.film_id, f.title, 
#                         COUNT(r.rental_id) AS rental_count
#                         FROM sakila.film f
#                         JOIN sakila.inventory i ON f.film_id = i.film_id
#                         JOIN sakila.rental r ON i.inventory_id = r.inventory_id
#                         JOIN sakila.film_actor fa ON f.film_id = fa.film_id
#                         JOIN actor_most am ON fa.actor_id = am.actor_id
#                         GROUP BY f.film_id, f.title
#                         ORDER BY  rental_count DESC
#                         LIMIT 5
#                     )
#                     SELECT film_id, title, rental_count
#                     FROM top_rented_movies;"""
#     cursor.execute(sql_query, (actor_id))
#     results = cursor.fetchall()
#     cursor.close()
#     db.close()
#     return jsonify(results)

# Customers Page Feature 1
# As a user I want to view a list of all customers (Pref. using pagination)
@app.route("/customers", methods=['GET'])
def customers():
    db = get_db()
    cursor = db.cursor()
    sql_query = """SELECT * FROM sakila.customer;"""
    cursor.execute(sql_query)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)



if __name__ == "__main__":
    app.run(debug=True, port=8080)