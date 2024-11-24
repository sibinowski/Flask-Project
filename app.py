import sqlite3
from flask import Flask, jsonify, request

app = Flask(__name__)

# Database connection function
def get_db_connection():
    """Establish a connection to the SQLite database."""
    conn = sqlite3.connect('users_vouchers.db')  # Ensure the path is correct for your setup
    conn.row_factory = sqlite3.Row  # This allows us to access columns by name
    return conn

# Database initialization function
def initialize_database():
    """Create necessary tables in the database if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create user_info table
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS user_info (
            user_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER
        )
    ''')

    # Create user_spending table
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS user_spending (
            user_id INTEGER,
            money_spent REAL,
            year INTEGER,
            FOREIGN KEY(user_id) REFERENCES user_info(user_id)
        )
    ''')

    # Create high_spenders table for users who exceed spending threshold
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS high_spenders (
            user_id INTEGER PRIMARY KEY,
            total_spending REAL,
            FOREIGN KEY(user_id) REFERENCES user_info(user_id)
        )
    ''')

    conn.commit()
    conn.close()

# Initialize database when the application starts
initialize_database()

# API Endpoints

# Endpoint 1: Retrieve Total Spending by User
@app.route('/total_spent/<int:user_id>', methods=['GET'])
def total_spent(user_id):
    """Fetch the total money spent by a specific user."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(money_spent) AS total_spending FROM user_spending WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()

    if row["total_spending"] is None:
        return jsonify({"message": "No spending data found for the user."}), 404
    return jsonify({"user_id": user_id, "total_spending": row["total_spending"]}), 200

# Endpoint 2: Calculate Average Spending by Age Range
@app.route('/average_spending_by_age', methods=['GET'])
def average_spending_by_age():
    """Fetch average spending per age range."""
    age_ranges = [(18, 24), (25, 30), (31, 36), (37, 47), (48, None)]
    results = {}

    conn = get_db_connection()
    cursor = conn.cursor()

    for start, end in age_ranges:
        if end:
            cursor.execute("""
                SELECT AVG(money_spent) AS avg_spending
                FROM user_spending
                JOIN user_info ON user_spending.user_id = user_info.user_id
                WHERE user_info.age BETWEEN ? AND ?
            """, (start, end))
        else:
            cursor.execute("""
                SELECT AVG(money_spent) AS avg_spending
                FROM user_spending
                JOIN user_info ON user_spending.user_id = user_info.user_id
                WHERE user_info.age >= ?
            """, (start,))

        avg_spent = cursor.fetchone()["avg_spending"]
        range_label = f"{start}-{end}" if end else f">{start}"
        results[range_label] = avg_spent or 0

    conn.close()
    return jsonify(results), 200

# Endpoint 3: Write User Data to High Spenders (with spending threshold)
@app.route('/write_high_spenders', methods=['POST'])
def write_high_spenders():
    """Insert user data into high_spenders if they meet the spending threshold."""
    data = request.get_json()
    user_id = data.get("user_id")
    total_spending = data.get("total_spending")
    spending_threshold = 1000  # Example threshold

    # Validate input data
    if not user_id or not total_spending:
        return jsonify({"message": "Invalid data format. Please provide user_id and total_spending."}), 400

    # Only insert if total spending exceeds the threshold
    if total_spending > spending_threshold:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO high_spenders (user_id, total_spending)
                VALUES (?, ?)
            """, (user_id, total_spending))

            conn.commit()
            conn.close()
            return jsonify({"message": "User data successfully inserted into high_spenders."}), 201

        except sqlite3.IntegrityError:
            return jsonify({"message": "User already exists in high_spenders."}), 409
    else:
        return jsonify({"message": "User spending does not meet the threshold of $1000."}), 400

# Running the Flask application
if __name__ == '__main__':
    app.run(debug=True)
