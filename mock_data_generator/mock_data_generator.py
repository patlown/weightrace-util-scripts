import sys
import psycopg2
from datetime import datetime, timedelta
import random
import json
from faker import Faker

fake = Faker()

def generate_user():
    return {
        "UserUid": fake.uuid4(),
        "FirstName": fake.first_name(),
        "LastName": fake.last_name(),
        "CreationDate": fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d"),
        "DOB": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
        "Email": fake.email(),
        "Phone": fake.phone_number(),
        "StartWeight": round(random.uniform(45, 200), 1),
    }

def generate_weights(user_id, num_weights):
    weights = []
    start_date = datetime.strptime(user_id["CreationDate"], "%Y-%m-%d")
    end_date = datetime.now()
    for _ in range(num_weights):
        log_date = fake.date_between_dates(date_start=start_date, date_end=end_date)
        value = round(random.uniform(45, 200), 1)
        weights.append({
            "LogDate": log_date.strftime("%Y-%m-%d"),
            "Value": value,
            "UserId": user_id,
        })
    return weights

def generate_mock_data(num_users, num_weights_per_user):
    users = [generate_user() for _ in range(num_users)]
    weights = [weight for user in users for weight in generate_weights(user, num_weights_per_user)]

    return {
        "Users": users,
        "Weights": weights,
    }

def write_to_json(mock_data):
    with open("mock_data.json", "w") as f:
        json.dump(mock_data, f, indent=2)

    print(f"Generated {len(mock_data['Users'])} users and {len(mock_data['Weights'])} weights in mock_data.json")

def write_to_database(mock_data, db_config):
    connection = None  # Add this line to initialize the variable
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        for user in mock_data['Users']:
            cursor.execute("""
                INSERT INTO "Users" (
                    "UserUid", "FirstName", "LastName", "CreationDate", "DOB",
                    "Email", "Phone", "StartWeight")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING "UserId";
            """, (
                user['UserUid'], user['FirstName'], user['LastName'],
                user['CreationDate'], user['DOB'], user['Email'],
                user['Phone'], user['StartWeight']
            ))
            user_id = cursor.fetchone()[0]

            for weight in filter(lambda w: w['UserId'] == user, mock_data['Weights']):
                cursor.execute("""
                    INSERT INTO "Weights" ("LogDate", "Value", "UserId")
                    VALUES (%s, %s, %s);
                """, (
                    weight['LogDate'], weight['Value'], user_id
                ))

        connection.commit()
        print(f"Inserted {len(mock_data['Users'])} users and {len(mock_data['Weights'])} weights into the database")

    except psycopg2.Error as e:
        print(f"Error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def load_db_config():
    with open("db_config.json") as f:
        return json.load(f)


def display_help():
    help_message = """Usage: python mock_data_generator.py <num_users> <num_weights_per_user> <output_method>

Arguments:
  <num_users>             Number of users to generate
  <num_weights_per_user>  Number of weights per user to generate
  <output_method>         Choose between 'json' or 'psql'

Output methods:
  json  Write mock data to a JSON file (mock_data.json)
  psql  Write mock data to a PostgreSQL database (requires db_config.json)

Example:
  python mock_data_generator.py 10 5 json
    """
    print(help_message)

if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] == "-h":
        display_help()
        sys.exit(1)

    num_users = int(sys.argv[1])
    num_weights_per_user = int(sys.argv[2])
    output_method = sys.argv[3]

    mock_data = generate_mock_data(num_users, num_weights_per_user)

    if output_method == "json":
        write_to_json(mock_data)
    elif output_method == "psql":
        db_config = load_db_config()
        write_to_database(mock_data, db_config)
    else:
        print("Invalid output method. Use 'json' to write to a file or 'psql' to write to a PostgreSQL database.")
        sys.exit(1)
