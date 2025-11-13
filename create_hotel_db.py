import sqlite3
import datetime
import random

def create_hotel_booking_db(db_name="hotel_booking_expanded.db"):
    """
    Creates and populates a SQLite database for a hotel booking system with an
    expanded network of hotels across multiple major cities.

    Args:
        db_name (str): The name of the SQLite database file to be created.
    """
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        print(f"Successfully connected to SQLite database: {db_name}")

        # Enable foreign key support
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Combined DDL and DML statements
        sql_script = """
        -- Drop views and tables if they exist for a clean build
        DROP VIEW IF EXISTS booking_details_view;
        DROP TABLE IF EXISTS bookings;
        DROP TABLE IF EXISTS room_inventory;
        DROP TABLE IF EXISTS pricing_rules;
        DROP TABLE IF EXISTS rooms;
        DROP TABLE IF EXISTS hotel_amenities;
        DROP TABLE IF EXISTS hotels;
        DROP TABLE IF EXISTS users;

        -- Users table with corporate distinction
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_code TEXT UNIQUE NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            is_corporate INTEGER DEFAULT 0, -- 1 for TRUE, 0 for FALSE
            company_name TEXT -- Populated if is_corporate is TRUE
        );

        -- Hotels table
        CREATE TABLE hotels (
            hotel_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_name TEXT NOT NULL,
            hotel_code TEXT UNIQUE NOT NULL,
            chain TEXT,
            star_rating INTEGER CHECK (star_rating BETWEEN 1 AND 5),
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT,
            country TEXT NOT NULL,
            postal_code TEXT,
            phone TEXT,
            email TEXT,
            corporate_discount_percent REAL DEFAULT 0,
            is_preferred_vendor INTEGER DEFAULT 0
        );

        -- Hotel amenities table
        CREATE TABLE hotel_amenities (
            amenity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_id INTEGER REFERENCES hotels(hotel_id) ON DELETE CASCADE,
            amenity_name TEXT NOT NULL,
            amenity_type TEXT
        );

        -- Rooms table
        CREATE TABLE rooms (
            room_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_id INTEGER REFERENCES hotels(hotel_id) ON DELETE CASCADE,
            room_type TEXT NOT NULL,
            base_price REAL NOT NULL,
            max_occupancy INTEGER DEFAULT 2,
            bed_type TEXT,
            amenities TEXT -- JSON array as a string
        );

        -- Pricing rules for dynamic pricing
        CREATE TABLE pricing_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_id INTEGER,
            season TEXT,
            day_of_week TEXT,
            price_multiplier REAL DEFAULT 1.0,
            priority INTEGER DEFAULT 0
        );

        -- Room inventory (daily availability)
        CREATE TABLE room_inventory (
            inventory_id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id INTEGER REFERENCES rooms(room_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            available_count INTEGER DEFAULT 0,
            price REAL,
            UNIQUE(room_id, date)
        );

        -- Bookings table
        CREATE TABLE bookings (
            booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_reference TEXT UNIQUE NOT NULL,
            user_id INTEGER REFERENCES users(user_id),
            hotel_id INTEGER REFERENCES hotels(hotel_id),
            room_id INTEGER REFERENCES rooms(room_id),
            check_in_date DATE NOT NULL,
            check_out_date DATE NOT NULL,
            nights INTEGER NOT NULL,
            guest_name TEXT,
            guest_count INTEGER DEFAULT 1,
            total_amount REAL NOT NULL,
            per_night_rate REAL NOT NULL,
            corporate_discount REAL DEFAULT 0,
            status TEXT DEFAULT 'pending',
            purpose_of_travel TEXT
        );
        
        -- Indexes for performance
        CREATE INDEX idx_users_email ON users(email);
        CREATE INDEX idx_hotels_city ON hotels(city);
        CREATE INDEX idx_bookings_user ON bookings(user_id);
        CREATE INDEX idx_inventory_date ON room_inventory(date);

        -- Insert Expanded Sample Data

        -- Users (mix of corporate and individual)
        INSERT INTO users (user_code, first_name, last_name, email, is_corporate, company_name) VALUES
        ('CORP001', 'John', 'Smith', 'john.smith@techcorp.com', 1, 'TechCorp Inc.'),
        ('CORP002', 'Jane', 'Doe', 'jane.doe@innovate.io', 1, 'Innovate Solutions'),
        ('CORP003', 'Peter', 'Jones', 'peter.jones@techcorp.com', 1, 'TechCorp Inc.'),
        ('INDV001', 'Mary', 'Williams', 'mary.w@email.com', 0, NULL),
        ('INDV002', 'David', 'Brown', 'd.brown@webmail.com', 0, NULL),
        ('CORP004', 'Emily', 'Clark', 'emily.clark@globalfinance.net', 1, 'Global Finance'),
        ('INDV003', 'Michael', 'Davis', 'mike.davis@mymail.com', 0, NULL),
        ('CORP005', 'Sarah', 'Miller', 'sarah.miller@innovate.io', 1, 'Innovate Solutions'),
        ('CORP006', 'Kevin', 'Wilson', 'kevin.wilson@datasys.com', 1, 'Data Systems LLC'),
        ('INDV004', 'Laura', 'Taylor', 'laura.t@fastmail.com', 0, NULL);

        -- Hotels (Expanded to 12 hotels in multiple cities)
        INSERT INTO hotels (hotel_name, hotel_code, chain, star_rating, address, city, state, country, postal_code, phone, email, corporate_discount_percent, is_preferred_vendor) VALUES
        ('Grand Plaza Hotel', 'GPH001', 'Grand Hotels', 5, '123 Business Center Dr', 'New York', 'NY', 'USA', '10001', '+1-212-555-0100', 'reservations@grandplazany.com', 15.00, 1),
        ('Corporate Inn Downtown', 'CID001', 'Corporate Inn', 4, '456 Corporate Blvd', 'San Francisco', 'CA', 'USA', '94105', '+1-415-555-0200', 'bookings@corporateinnsf.com', 18.00, 1),
        ('Business Suites Central', 'BSC001', 'Business Suites', 4, '789 Central Ave', 'Chicago', 'IL', 'USA', '60601', '+1-312-555-0300', 'info@businesssuiteschi.com', 20.00, 1),
        ('Comfort Stay Inn', 'CSI001', 'Comfort Stay', 3, '555 Main Street', 'Austin', 'TX', 'USA', '78701', '+1-512-555-0500', 'contact@comfortstayatx.com', 10.00, 0),
        ('Mountain View Lodge', 'MVL001', 'Independent', 4, '111 Skyline Dr', 'Denver', 'CO', 'USA', '80202', '+1-303-555-0800', 'reservations@mountainview.com', 12.00, 1),
        ('Ocean Breeze Resort', 'OBR001', 'Sunshine Resorts', 5, '999 Beachfront Blvd', 'Miami', 'FL', 'USA', '33139', '+1-305-555-0900', 'bookings@oceanbreezemiami.com', 10.00, 0),
        ('Sunset Marquis LA', 'SML001', 'Independent', 5, '1200 Alta Loma Rd', 'Los Angeles', 'CA', 'USA', '90069', '+1-310-555-1212', 'contact@sunsetmarquisla.com', 15.00, 1),
        ('Emerald City Inn', 'ECI001', 'City Stays', 4, '200 Pine St', 'Seattle', 'WA', 'USA', '98101', '+1-206-555-3434', 'info@emeraldcityinn.com', 12.00, 0),
        ('Freedom Trail Hotel', 'FTH001', 'Heritage Hotels', 4, '50 Beacon St', 'Boston', 'MA', 'USA', '02108', '+1-617-555-1776', 'reservations@freedomtrailhotel.com', 14.00, 1),
        ('The Oasis Resort & Casino', 'ORC001', 'Paradise Resorts', 5, '3500 Las Vegas Blvd S', 'Las Vegas', 'NV', 'USA', '89109', '+1-702-555-7777', 'bookings@oasisvegas.com', 10.00, 0),
        ('Magic Kingdom Gateway', 'MKG001', 'Family Stays', 3, '7000 Irlo Bronson Hwy', 'Orlando', 'FL', 'USA', '34747', '+1-407-555-5000', 'info@magickingdomgateway.com', 8.00, 0),
        ('Texas Grande Hotel', 'TGH001', 'Grand Hotels', 5, '1900 N Akard St', 'Dallas', 'TX', 'USA', '75201', '+1-214-555-8000', 'concierge@texasgrande.com', 18.00, 1);

        -- Hotel amenities for all 12 hotels
        INSERT INTO hotel_amenities (hotel_id, amenity_name, amenity_type) VALUES
        (1, 'Free High-Speed WiFi', 'wifi'), (1, 'Rooftop Restaurant', 'dining'), (1, 'Valet Parking', 'parking'),
        (2, 'Free WiFi', 'wifi'), (2, 'On-Site Parking', 'parking'), (2, 'Complimentary Breakfast', 'breakfast'),
        (3, 'Free WiFi', 'wifi'), (3, 'Indoor Pool', 'pool'), (3, 'Conference Rooms', 'business'),
        (4, 'Free WiFi', 'wifi'), (4, 'Free Parking', 'parking'), (4, 'Continental Breakfast', 'breakfast'),
        (5, 'Free WiFi', 'wifi'), (5, 'Ski Valet', 'recreation'), (5, 'Hot Tub', 'pool'),
        (6, 'Beach Access', 'recreation'), (6, 'Outdoor Pool', 'pool'), (6, 'Spa Services', 'spa'),
        (7, 'Free WiFi', 'wifi'), (7, 'Recording Studio', 'business'), (7, 'Heated Pool', 'pool'),
        (8, 'Free WiFi', 'wifi'), (8, 'Rooftop Bar', 'dining'), (8, 'Pet Friendly', 'general'),
        (9, 'Free WiFi', 'wifi'), (9, 'Historic Tours', 'recreation'), (9, 'Fine Dining Restaurant', 'dining'),
        (10, 'Free WiFi', 'wifi'), (10, 'Casino Floor', 'recreation'), (10, 'Multiple Pools', 'pool'),
        (11, 'Free WiFi', 'wifi'), (11, 'Shuttle to Parks', 'transport'), (11, 'Kids Play Area', 'recreation'),
        (12, 'Free WiFi', 'wifi'), (12, 'Steakhouse', 'dining'), (12, 'Infinity Pool', 'pool');

        -- Rooms for all 12 hotels
        INSERT INTO rooms (hotel_id, room_type, base_price, max_occupancy, bed_type, amenities) VALUES
        (1, 'Standard King', 275.00, 2, 'King', '["wifi", "tv", "minibar"]'),
        (2, 'Queen Room', 190.00, 2, 'Queen', '["wifi", "tv", "desk"]'),
        (3, 'Business King', 210.00, 2, 'King', '["wifi", "tv", "desk"]'),
        (4, 'Standard Double', 130.00, 4, 'Two Doubles', '["wifi", "tv"]'),
        (5, 'Mountain View King', 220.00, 2, 'King', '["wifi", "tv", "fireplace"]'),
        (6, 'Oceanfront Balcony', 350.00, 2, 'King', '["wifi", "tv", "balcony"]'),
        (7, 'Deluxe King', 320.00, 2, 'King', '["wifi", "tv", "minibar"]'),
        (7, 'Villa Suite', 750.00, 4, 'Two Kings', '["wifi", "tv", "private_pool"]'),
        (8, 'City View Queen', 240.00, 2, 'Queen', '["wifi", "tv", "desk"]'),
        (8, 'Corner Suite', 420.00, 3, 'King + Sofa', '["wifi", "tv", "living_area"]'),
        (9, 'Historic Double', 260.00, 4, 'Two Doubles', '["wifi", "tv", "antique_furniture"]'),
        (10, 'Resort King', 180.00, 2, 'King', '["wifi", "tv", "minibar"]'),
        (10, 'High Roller Suite', 950.00, 4, 'Two Kings', '["wifi", "tv", "bar", "lounge"]'),
        (11, 'Family Room', 160.00, 4, 'Two Queens', '["wifi", "tv", "fridge"]'),
        (12, 'Grande King', 350.00, 2, 'King', '["wifi", "tv", "luxury_bath"]');

        -- Pricing rules for all hotels
        INSERT INTO pricing_rules (hotel_id, season, day_of_week, price_multiplier, priority) VALUES
        (1, 'peak', 'weekend', 1.50, 10),
        (2, 'peak', NULL, 1.30, 8),
        (5, 'peak', NULL, 1.60, 10), -- Ski season in Denver
        (6, 'peak', 'weekend', 1.75, 12), -- Holiday weekend in Miami
        (10, NULL, 'weekend', 1.80, 15), -- Las Vegas on a weekend
        (11, 'peak', NULL, 1.65, 12); -- Summer/holidays in Orlando

        -- Bookings (more records using new hotels)
        INSERT INTO bookings (booking_reference, user_id, hotel_id, room_id, check_in_date, check_out_date, nights, guest_name, total_amount, per_night_rate, status, purpose_of_travel, corporate_discount) VALUES
        ('TC-2025-001', 1, 1, 1, '2025-11-20', '2025-11-23', 3, 'John Smith', 701.25, 275.00, 'confirmed', 'Client Meeting', 123.75),
        ('IS-2025-001', 2, 2, 2, '2025-12-05', '2025-12-07', 2, 'Jane Doe', 311.60, 190.00, 'confirmed', 'Marketing Conference', 68.40),
        ('IV-2025-001', 4, 6, 6, '2026-01-10', '2026-01-15', 5, 'Mary Williams', 1750.00, 350.00, 'completed', 'Vacation', 0),
        ('GF-2025-001', 6, 3, 3, '2025-12-10', '2025-12-12', 2, 'Emily Clark', 336.00, 210.00, 'confirmed', 'Annual Audit', 84.00),
        ('DS-2025-001', 9, 12, 15, '2025-11-25', '2025-11-28', 3, 'Kevin Wilson', 924.00, 350.00, 'confirmed', 'Sales Kick-off', 189.00),
        ('IV-2025-003', 10, 10, 12, '2026-02-20', '2026-02-23', 3, 'Laura Taylor', 540.00, 180.00, 'pending', 'Personal Trip', 0),
        ('TC-2025-003', 3, 9, 11, '2026-03-15', '2026-03-18', 3, 'Peter Jones', 686.40, 260.00, 'pending', 'Historical Site Visit', 122.40),
        ('IV-2025-004', 7, 11, 14, '2026-04-10', '2026-04-15', 5, 'Michael Davis', 800.00, 160.00, 'confirmed', 'Family Vacation', 0);
        
        -- View for Booking Details with User Info
        CREATE VIEW booking_details_view AS
        SELECT
            b.booking_id,
            b.booking_reference,
            u.first_name || ' ' || u.last_name as user_name,
            u.is_corporate,
            u.company_name,
            h.hotel_name,
            h.city,
            r.room_type,
            b.check_in_date,
            b.check_out_date,
            b.total_amount,
            b.status
        FROM bookings b
        JOIN users u ON b.user_id = u.user_id
        JOIN hotels h ON b.hotel_id = h.hotel_id
        JOIN rooms r ON b.room_id = r.room_id;
        """

        # Execute the entire SQL script
        cursor.executescript(sql_script)
        print("Tables, indexes, view, and expanded sample data created successfully.")

        # --- Python logic to populate room_inventory for the next 90 days ---
        print("Populating room inventory...")

        cursor.execute("SELECT room_id, base_price FROM rooms")
        all_rooms = cursor.fetchall()

        inventory_records = []
        start_date = datetime.date.today()

        for room_id, base_price in all_rooms:
            for i in range(90):
                inventory_date = start_date + datetime.timedelta(days=i)
                available_count = random.randint(2, 20)
                inventory_records.append((room_id, inventory_date.strftime('%Y-%m-%d'), available_count, base_price))

        cursor.executemany(
            "INSERT INTO room_inventory (room_id, date, available_count, price) VALUES (?, ?, ?, ?)",
            inventory_records
        )
        print(f"Inserted {len(inventory_records)} room inventory records.")

        # Commit changes and close connection
        conn.commit()
        print("Database changes have been committed.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            print("SQLite connection is closed.")

if __name__ == '__main__':
    create_hotel_booking_db()
