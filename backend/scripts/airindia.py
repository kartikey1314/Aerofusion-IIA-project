from pymongo import MongoClient
import random, datetime


client = MongoClient("mongodb+srv://kartikey24046:kartikey123@cluster0.zrknx9w.mongodb.net/aerofusion_db?retryWrites=true&w=majority")
db = client["Aerofusion_db"]
coll = db["airindia_flights"]



cities = [
    "Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Kolkata",
    "Chennai", "Pune", "Goa", "Ahmedabad", "Jaipur",
    "New York", "London", "Sydney", "Melbourne", "San Francisco",
    "Los Angeles", "Chicago", "Manchester", "Perth", "Boston"
]

offers = [
    ("MahaFestive", 10),
    ("WinterBonanza", 15),
    ("FlyHigh5", 5),
    ("DreamDeal20", 20),
    (None, None)
]

def generate_seats():
    rows = range(10, 31)
    seats = [f"{r}{c}" for r in rows for c in ['A','B','C','D','E','F']]
    return random.sample(seats, random.randint(10, 35))

def random_time(start_hour, end_hour):
    h = random.randint(start_hour, end_hour)
    m = random.choice([0, 15, 30, 45])
    return f"{h:02d}:{m:02d}"

base_date = datetime.date(2025, 10, 12)
documents = []

num_routes = 50 
for i in range(num_routes):
    flight_no = f"AI{200 + i}"
    from_city, to_city = random.sample(cities, 2)
    dep_hour = random.randint(5, 21)
    dep_time = random_time(dep_hour, dep_hour + 4)
    duration = random.randint(120, 900)
    arr_time = (datetime.datetime.combine(datetime.date.today(), datetime.time(dep_hour, 0))
                + datetime.timedelta(minutes=duration)).time()
    arr_time_str = arr_time.strftime("%H:%M")

    for d in range(random.randint(3, 5)):
        date = base_date + datetime.timedelta(days=d)
        base_price = random.randint(5000, 95000)
        offer_name, discount = random.choice(offers)
        seats = generate_seats()

        doc = {
            "flight_number": flight_no,
            "airline_name": "Air India",
            "route": {"origin": from_city, "destination": to_city},
            "schedule": {
                "date": str(date),
                "departure": dep_time,
                "arrival": arr_time_str,
                "duration_min": duration
            },
            "pricing": {
                "base_price": base_price,
                "offer": {"name": offer_name, "discount": discount},
                "currency": "INR"
            },
            "aircraft_type": random.choice(["B787 Dreamliner", "A320neo", "B777"]),
            "availability": {"seats": seats},
            "booking": {
                "link": f"https://book.airindia.com/{flight_no}/{from_city}-{to_city}/{date}",
                "last_updated": datetime.datetime.utcnow()
            }
        }

        documents.append(doc)


coll.insert_many(documents)
print("Inserted {len(documents)} Air India documents into MongoDB.")
