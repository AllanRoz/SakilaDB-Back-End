from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
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
@app.route("/details/top5films", methods=['POST'])
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

@app.route("/details/top5actors", methods=['POST'])
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



# Films Page Feature 1
# As a user I want to be able to search a film by name of film, name of an actor, or genre of the film
@app.route("/films", methods=['GET'])
def films():
    db = get_db()
    cursor = db.cursor()
    sql_query = """SELECT f.film_id, f.title AS film_title, f.release_year,
                    GROUP_CONCAT(DISTINCT c.name ORDER BY c.name SEPARATOR ', ') AS genres,
                    GROUP_CONCAT(DISTINCT CONCAT(a.first_name, ' ', a.last_name) ORDER BY a.last_name SEPARATOR ', ') AS actors
                    FROM sakila.film f
                    JOIN sakila.film_actor fa ON f.film_id = fa.film_id
                    JOIN sakila.actor a ON fa.actor_id = a.actor_id
                    JOIN sakila.film_category fc ON f.film_id = fc.film_id
                    JOIN sakila.category c ON fc.category_id = c.category_id
                    GROUP BY f.film_id, f.title, f.release_year
                    ORDER BY f.title;"""
    cursor.execute(sql_query)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

# Films Page Feature 2
# As a user I want to be able to view details of the film
@app.route("/details/filmdata", methods=['POST'])
def filmsData():
    db = get_db()
    cursor = db.cursor()

    # Getting data film_id
    data = request.get_json()
    film_id = data['film_id']
    sql_query = """SELECT f.film_id, f.description, f.release_year, f.rental_duration, f.rental_rate, f.length, f.replacement_cost, 
                    f.rating, f.special_features,f.last_update,
                    GROUP_CONCAT(DISTINCT CONCAT(a.first_name, ' ', a.last_name) ORDER BY a.last_name SEPARATOR ', ') AS actors
                    FROM sakila.film f
                    JOIN sakila.film_actor fa ON f.film_id = fa.film_id
                    JOIN sakila.actor a ON fa.actor_id = a.actor_id
                    WHERE f.film_id = %s
                    GROUP BY f.film_id, f.description, f.release_year, f.rental_duration, f.rental_rate, 
                            f.length, f.replacement_cost, f.rating, f.special_features, f.last_update"""
    cursor.execute(sql_query, film_id)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

# Films Page Feature 3
# As a user I want to be able to rent a film out to a customer
@app.route("/rentFilm", methods=['POST'])
def rentFilm():
    # Get the database connection and cursor
    db = get_db()
    cursor = db.cursor()

    # Getting data from the request
    data = request.get_json()
    customer_id = data['customer_id']
    film_id = data['film_id']
    print(customer_id)
    print(film_id)

    # Prepare the SQL query to create a rental
    sql_query = """
    INSERT INTO sakila.rental (customer_id, inventory_id, rental_date, return_date, staff_id)
    SELECT %s, i.inventory_id, NOW(), NULL, 1
    FROM sakila.inventory i
    WHERE i.film_id = %s
    AND i.inventory_id NOT IN (
        SELECT r.inventory_id 
        FROM sakila.rental r
        WHERE r.return_date IS NULL
    )
    ORDER BY i.inventory_id ASC
    LIMIT 1;
    """
    
    # Execute the query
    cursor.execute(sql_query, (customer_id, film_id))

    # Commit the transaction
    db.commit()

    # Close the cursor and connection
    cursor.close()
    db.close()

    # Return success response
    return jsonify({"message": "Film rented"})

# Films Page Feature 3
# Check Film Availability
@app.route('/checkFilmAvailability', methods=['POST'])
def check_film_availability():
    data = request.get_json()
    film_id = data['film_id']

    db = get_db()
    cursor = db.cursor()

    # Query to check available stock
    sql_query = """
        SELECT COUNT(*) AS available_stock 
        FROM sakila.inventory 
        WHERE film_id = %s AND inventory_id NOT IN (
            SELECT inventory_id FROM sakila.rental WHERE return_date IS NULL
        );
    """
    cursor.execute(sql_query, (film_id))
    result = cursor.fetchone()

    if result['available_stock'] > 0:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False})

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

# Customers Page Feature 3
# As a user I want to be able to add a new customer
@app.route("/customer/add", methods=['POST'])
def add_customer():
    db = get_db()
    cursor = db.cursor()
    
    # Get data from the request
    data = request.get_json()

    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')
    phone = data.get('phone')
    address = data.get('address')
    address2 = data.get('address2')
    district = data.get('district')
    city = data.get('city')
    country = data.get('country')
    postal_code = data.get('postal_code')

    if(first_name == '' or last_name == '' or email == '' or phone == '' or address == '' or district == '' or city == '' or postal_code == ''):
        return jsonify({'message': 'Field empty'})

    # Check if email already exists
    cursor.execute("SELECT email FROM customer WHERE email = %s", (email,))
    if cursor.fetchone():
        return jsonify({'message': 'Email already exists'})

    # Check if the country exists
    cursor.execute("SELECT country_id FROM country WHERE country = %s", (country,))
    country_row = cursor.fetchone()
    if country_row:
        country_id = country_row['country_id']
    else:
        cursor.execute("INSERT INTO country (country, last_update) VALUES (%s, %s)", (country, datetime.utcnow()))
        db.commit()
        country_id = cursor.lastrowid

    # Check if the city exists
    cursor.execute("SELECT city_id FROM city WHERE city = %s AND country_id = %s", (city, country_id))
    city_row = cursor.fetchone()
    if city_row:
        city_id = city_row['city_id']
    else:
        cursor.execute("INSERT INTO city (city, country_id, last_update) VALUES (%s, %s, %s)", (city, country_id, datetime.utcnow()))
        db.commit()
        city_id = cursor.lastrowid

    # Check if the address exists
    cursor.execute("SELECT address_id FROM address WHERE address = %s AND city_id = %s", (address, city_id))
    address_row = cursor.fetchone()
    if address_row:
        address_id = address_row['address_id']
    else:
        cursor.execute("""
            INSERT INTO address (address, address2, district, city_id, postal_code, phone, location, last_update) 
            VALUES (%s, %s, %s, %s, %s, %s, POINT(0,0), NOW())
        """, (address, address2, district, city_id, postal_code, phone))
        db.commit()
        address_id = cursor.lastrowid

    # Insert new customer
    cursor.execute("""
        INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date, last_update)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
    """, (1, first_name, last_name, email, address_id, 1))
    db.commit()

    cursor.close()
    db.close()

    return jsonify({'message': 'Customer added'})

# Customers Page Feature 4
# As a user I want to be able to edit a customer’s details
@app.route("/customer/update", methods=['PUT'])
def updateCustomer():
    db = get_db()
    cursor = db.cursor()
    
    # Get data from the request
    data = request.get_json()

    customer_id = data.get('customer_id')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')
    phone = data.get('phone')
    address = data.get('address')
    address2 = data.get('address2')
    district = data.get('district')
    city = data.get('city')
    country = data.get('country')
    postal_code = data.get('postal_code')

    if(first_name == '' or last_name == '' or email == '' or phone == '' or address == '' or district == '' or city == '' or postal_code == ''):
        return jsonify({'message': 'Field empty'})

    # Fetch the current customer data from the database
    cursor.execute("SELECT email, address_id FROM customer WHERE customer_id = %s", (customer_id,))
    customer = cursor.fetchone()

    if not customer:
        return jsonify({'message': 'Customer not found'}), 404
    
    current_email = customer['email']

    if email != current_email:
        cursor.execute("SELECT email FROM customer WHERE email = %s", (email,))
        if cursor.fetchone():
            return jsonify({'message': 'Email already exists'}), 400

    cursor.execute("SELECT country_id FROM country WHERE country = %s", (country,))
    country_row = cursor.fetchone()
    if country_row:
        country_id = country_row['country_id']
    else:
        cursor.execute("INSERT INTO country (country, last_update) VALUES (%s, %s)", (country, datetime.utcnow()))
        db.commit()
        country_id = cursor.lastrowid

    cursor.execute("SELECT city_id FROM city WHERE city = %s AND country_id = %s", (city, country_id))
    city_row = cursor.fetchone()
    if city_row:
        city_id = city_row['city_id']
    else:
        cursor.execute("INSERT INTO city (city, country_id, last_update) VALUES (%s, %s, %s)", (city, country_id, datetime.utcnow()))
        db.commit()
        city_id = cursor.lastrowid

    cursor.execute("SELECT address_id FROM address WHERE address = %s AND city_id = %s", (address, city_id))
    address_row = cursor.fetchone()
    if address_row:
        address_id = address_row['address_id']
        cursor.execute("""
            UPDATE address
            SET address = %s, address2 = %s, district = %s, postal_code = %s, phone = %s, last_update = NOW()
            WHERE address_id = %s
        """, (address, address2, district, postal_code, phone, address_id))
    else:
        cursor.execute("""
            INSERT INTO address (address, address2, district, city_id, postal_code, phone, location, last_update) 
            VALUES (%s, %s, %s, %s, %s, %s, POINT(0,0), NOW())
        """, (address, address2, district, city_id, postal_code, phone))
        db.commit()
        address_id = cursor.lastrowid

    # Update the customer record
    cursor.execute("""
        UPDATE customer 
        SET first_name = %s, last_name = %s, email = %s, address_id = %s, last_update = NOW() 
        WHERE customer_id = %s
    """, (first_name, last_name, email, address_id, customer_id))
    db.commit()

    cursor.close()
    db.close()

    return jsonify({'message': 'Customer updated'}), 200


# Customers Page Feature 5
# As a user I want to be able to delete a customer if they no longer wish to patron at store
@app.route("/deleteCustomer", methods=['POST'])
def deleteCustomer():
    db = get_db()
    cursor = db.cursor()

    # Getting data
    data = request.get_json()
    customer_id = data['customer_id']

    sql_delete_payments = "DELETE FROM sakila.payment WHERE customer_id = %s;"
    sql_delete_rentals = "DELETE FROM sakila.rental WHERE customer_id = %s;"
    sql_delete_customer = "DELETE FROM sakila.customer WHERE customer_id = %s;"

    cursor.execute(sql_delete_payments, (customer_id,))
    cursor.execute(sql_delete_rentals, (customer_id,))
    cursor.execute(sql_delete_customer, (customer_id,))

    db.commit()
    rows_deleted = cursor.rowcount
    cursor.close()
    db.close()

    if rows_deleted > 0:
        return jsonify({"message": "Customer deleted successfully"})
    else:
        return jsonify({"message": "Customer could not be deleted or does not exist"})

# Customers Page Feature 6
# As a user I want to be able to view details of the film
@app.route("/details/customerdata", methods=['POST'])
def customersData():
    db = get_db()
    cursor = db.cursor()

    # Getting data film_id
    data = request.get_json()
    customer_id = data['customer_id']
    sql_query = """SELECT 
    c.customer_id, 
    c.first_name, 
    c.last_name, 
    c.email, 
    a.address, 
    a.address2,
    a.district, 
    a.postal_code, 
    a.phone, 
    ci.city, 
    co.country, 
    c.create_date,
    c.last_update,
    SUM(CASE WHEN r.return_date IS NOT NULL THEN 1 ELSE 0 END) AS rented,
    SUM(CASE WHEN r.return_date IS NULL AND r.rental_date IS NOT NULL THEN 1 ELSE 0 END) AS renting
FROM sakila.customer c
JOIN sakila.address a ON c.address_id = a.address_id
JOIN sakila.city ci ON a.city_id = ci.city_id
JOIN sakila.country co ON ci.country_id = co.country_id
LEFT JOIN sakila.rental r ON c.customer_id = r.customer_id
LEFT JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
LEFT JOIN sakila.film f ON i.film_id = f.film_id
WHERE c.customer_id = %s;
"""
    cursor.execute(sql_query, customer_id)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

# Customers Page Feature 7
# As a user I want to be able to indicate that a customer has returned a rented movie
@app.route("/returnFilm", methods=['POST'])
def returnFilm():
    db = get_db()
    cursor = db.cursor()

    # Getting data
    data = request.get_json()
    customer_id = data['customer_id']
    film_id = data['film_id']

    sql_query = """UPDATE sakila.rental 
    SET return_date = NOW()
    WHERE rental_id = (
        SELECT rental_id FROM (
            SELECT r.rental_id
            FROM sakila.rental r
            JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
            WHERE r.customer_id = %s
            AND i.film_id = %s
            AND r.return_date IS NULL
            ORDER BY r.rental_date ASC
            LIMIT 1
        ) AS subquery
    );
    """

    cursor.execute(sql_query, (customer_id, film_id))
    db.commit()
    cursor.close()
    db.close()

    if cursor.rowcount > 0:
        return jsonify({"message": "Film returned"})
    else:
        return jsonify({"message": "Film could not be returned"})

    




if __name__ == "__main__":
    app.run(debug=True, port=8080)