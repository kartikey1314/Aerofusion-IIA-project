import psycopg2, random, datetime


conn = psycopg2.connect(
    dbname="aerofusion_db",    
    user="master_user",         
    password="Kartikey@123",   
    host="localhost",
    port="5432"
)
cur = conn.cursor()

cities = [
    "Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Kolkata",
    "Chennai", "Pune", "Goa", "Ahmedabad", "Jaipur",
    "New York", "London", "Sydney", "Melbourne", "San Francisco",
    "Los Angeles", "Chicago", "Manchester", "Perth", "Boston"
]


offers = [
    ("Festive10", 10),
    ("Weekend5", 5),
    ("SummerSale15", 15),
    ("MegaOffer20", 20),
    (None, None)
]


def generate_seats():
    rows = range(10, 31)
    seats = [f"{r}{c}" for r in rows for c in ['A','B','C','D','E','F']]
    return random.sample(seats, random.randint(10, 35))

def random_time(start_hour, end_hour):
    h = random.randint(start_hour, end_hour)
    m = random.choice([0, 15, 30, 45])
    return datetime.time(h, m)


num_flights = 60 
base_date = datetime.date(2025, 10, 12)
records = []
flight_counter = 0

for i in range(num_flights):

    from_city, to_city = random.sample(cities, 2)
    flight_no = f"6E{100 + i}"
    dep_time = random_time(5, 21)
    duration = random.randint(90, 720)
    arr_time = (datetime.datetime.combine(datetime.date.today(), dep_time)
                + datetime.timedelta(minutes=duration)).time()

    days = random.randint(3, 5)
    for d in range(days):
        journey_date = base_date + datetime.timedelta(days=d)
        fare = random.randint(3000, 90000)
        offer_name, discount = random.choice(offers)
        booking_link = f"https://book.indigo.com/{flight_no}/{from_city}-{to_city}/{journey_date}"
        seats = generate_seats()

        records.append((
            flight_no, "IndiGo", from_city, to_city, journey_date,
            dep_time, arr_time, duration, fare, discount, offer_name,
            booking_link, seats
        ))
        flight_counter += 1

insert_query = """
INSERT INTO indigo_src (
    flight_no, airline, from_city, to_city, journey_date,
    departure_time, arrival_time, duration_mins,
    fare, discount_percent, offer_name, booking_link, seats
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
cur.executemany(insert_query, records)
conn.commit()

cur.close()
conn.close()
print("Inserted {flight_counter} rows into indigo_src successfully.")
