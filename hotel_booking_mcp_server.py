#!/usr/bin/env python3
"""
Hotel Booking FastMCP Server (Simplified - No Pre-Registration Required)
PostgreSQL/Neon Database Version

Completely flexible: No need to pre-register users!
Just provide name, email, and corporate status when booking.
"""

import psycopg2
import psycopg2.extras
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
from contextlib import contextmanager
import random
import os

from fastmcp import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hotel-booking-mcp")

# Database configuration
DB_URL = os.getenv("DB_URL")

# Initialize FastMCP server
mcp = FastMCP(
    name="hotel-booking-mcp",
    instructions="""
    This server provides hotel booking capabilities with PostgreSQL/Neon backend.
    
    No pre-registration required! Just provide:
    - Guest name and email
    - Corporate status (if applicable)
    - Company name (if corporate)
    
    Corporate discounts are applied automatically based on hotel agreements.
    """
)


# ============================================================================
# DATABASE CONNECTION MANAGEMENT
# ============================================================================

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def generate_booking_reference() -> str:
    """Generate a unique booking reference"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = random.randint(1000, 9999)
    return f"HTL-{timestamp}-{random_suffix}"


def calculate_nights(check_in: str, check_out: str) -> int:
    """Calculate number of nights between dates"""
    check_in_date = datetime.strptime(check_in, "%Y-%m-%d")
    check_out_date = datetime.strptime(check_out, "%Y-%m-%d")
    return (check_out_date - check_in_date).days


def is_weekend(date_str: str) -> bool:
    """Check if a date falls on weekend (Saturday=5, Sunday=6)"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.weekday() in [5, 6]


def get_season(date_str: str) -> str:
    """Determine season for a given date"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    month = date_obj.month
    
    # Peak season: June-August (summer) and December-January (holidays)
    if month in [6, 7, 8, 12, 1]:
        return "peak"
    else:
        return "regular"


def calculate_dynamic_price(
    base_price: float,
    check_in: str,
    check_out: str,
    hotel_id: int,
    is_corporate: bool,
    corporate_discount: float
) -> dict:
    """Calculate dynamic pricing with all factors"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT season, day_of_week, price_multiplier, priority
            FROM pricing_rules
            WHERE hotel_id = %s OR hotel_id IS NULL
            ORDER BY priority DESC
        """, (hotel_id,))
        
        pricing_rules = cursor.fetchall()
    
    nights = calculate_nights(check_in, check_out)
    total_price = 0
    
    current_date = datetime.strptime(check_in, "%Y-%m-%d")
    end_date = datetime.strptime(check_out, "%Y-%m-%d")
    
    while current_date < end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        night_price = base_price
        multiplier = 1.0
        
        season = get_season(date_str)
        is_weekend_day = is_weekend(date_str)
        day_type = "weekend" if is_weekend_day else "weekday"
        
        for rule in pricing_rules:
            rule_season = rule["season"]
            rule_day = rule["day_of_week"]
            rule_multiplier = rule["price_multiplier"]
            
            applies = True
            if rule_season and rule_season != season:
                applies = False
            if rule_day and rule_day != day_type:
                applies = False
            
            if applies:
                multiplier = float(rule_multiplier)
                break
        
        night_price *= multiplier
        total_price += night_price
        current_date += timedelta(days=1)
    
    discount_amount = 0
    if is_corporate and corporate_discount > 0:
        discount_amount = total_price * (float(corporate_discount) / 100)
        total_price -= discount_amount
    
    breakdown = [{
        "base_total": float(base_price) * nights,
        "after_dynamic_pricing": total_price + discount_amount,
        "corporate_discount_percent": float(corporate_discount) if is_corporate else 0,
        "corporate_discount_amount": round(discount_amount, 2),
        "final_total": round(total_price, 2)
    }]
    
    return {
        "total_price": round(total_price, 2),
        "average_per_night": round(total_price / nights, 2),
        "nights": nights,
        "breakdown": breakdown,
        "corporate_discount_applied": round(discount_amount, 2)
    }


# ============================================================================
# TOOL IMPLEMENTATIONS
# ============================================================================

@mcp.tool
def search_hotels(
    city: str,
    check_in_date: str,
    check_out_date: str,
    state: Optional[str] = None,
    preferred_only: bool = False,
    min_star_rating: Optional[int] = None,
    max_price: Optional[float] = None,
    is_corporate: bool = False
) -> str:
    """Search for hotels by city. No user registration required!
    
    Args:
        city: City name to search hotels in
        check_in_date: Check-in date in YYYY-MM-DD format
        check_out_date: Check-out date in YYYY-MM-DD format
        state: State/province (optional)
        preferred_only: Show only preferred vendor hotels
        min_star_rating: Minimum star rating (1-5)
        max_price: Maximum price per night
        is_corporate: Is this a corporate booking? (affects pricing display)
    
    Returns:
        JSON string with available hotels and pricing
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Build hotel search query
        query = """
            SELECT h.*, COUNT(DISTINCT r.room_id) as room_count
            FROM hotels h
            LEFT JOIN rooms r ON h.hotel_id = r.hotel_id
            WHERE LOWER(h.city) = LOWER(%s)
        """
        params = [city]
        
        if state:
            query += " AND LOWER(h.state) = LOWER(%s)"
            params.append(state)
        
        if preferred_only:
            query += " AND h.is_preferred_vendor = TRUE"
        
        if min_star_rating:
            query += " AND h.star_rating >= %s"
            params.append(min_star_rating)
        
        query += " GROUP BY h.hotel_id ORDER BY h.is_preferred_vendor DESC, h.star_rating DESC"
        
        cursor.execute(query, params)
        hotels = cursor.fetchall()
        
        results = []
        for hotel in hotels:
            hotel_dict = dict(hotel)
            hotel_id = hotel["hotel_id"]
            
            # Get rooms with availability
            cursor.execute("""
                SELECT r.*, MIN(ri.available_count) as min_availability
                FROM rooms r
                JOIN room_inventory ri ON r.room_id = ri.room_id
                WHERE r.hotel_id = %s
                    AND ri.date >= %s
                    AND ri.date < %s
                    AND ri.available_count > 0
                GROUP BY r.room_id, r.hotel_id, r.room_type, r.base_price, 
                         r.max_occupancy, r.bed_type, r.amenities
                HAVING MIN(ri.available_count) > 0
                ORDER BY r.base_price ASC
            """, (hotel_id, check_in_date, check_out_date))
            
            rooms = cursor.fetchall()
            rooms_with_pricing = []
            
            for room in rooms:
                room_dict = dict(room)
                
                pricing = calculate_dynamic_price(
                    float(room["base_price"]),
                    check_in_date,
                    check_out_date,
                    hotel_id,
                    is_corporate,
                    float(hotel["corporate_discount_percent"])
                )
                
                room_dict.update({
                    "calculated_total": pricing["total_price"],
                    "average_per_night": pricing["average_per_night"],
                    "nights": pricing["nights"],
                    "corporate_discount_applied": pricing["corporate_discount_applied"],
                    "available_rooms": room["min_availability"]
                })
                
                # Convert Decimal to float for JSON serialization
                room_dict["base_price"] = float(room_dict["base_price"])
                
                if max_price is None or room_dict["average_per_night"] <= max_price:
                    rooms_with_pricing.append(room_dict)
            
            if rooms_with_pricing:
                # Convert Decimal fields to float
                hotel_dict["corporate_discount_percent"] = float(hotel_dict.get("corporate_discount_percent", 0))
                hotel_dict["rooms"] = rooms_with_pricing
                hotel_dict["available_room_types"] = len(rooms_with_pricing)
                results.append(hotel_dict)
    
    return json.dumps({
        "search_criteria": {
            "city": city,
            "state": state,
            "check_in_date": check_in_date,
            "check_out_date": check_out_date,
            "is_corporate_booking": is_corporate,
            "preferred_only": preferred_only
        },
        "results_count": len(results),
        "hotels": results
    }, indent=2, default=str)


@mcp.tool
def get_hotel_details(
    hotel_id: int,
    check_in_date: str,
    check_out_date: str,
    is_corporate: bool = False
) -> str:
    """Get detailed hotel information.
    
    Args:
        hotel_id: Hotel ID
        check_in_date: Check-in date for pricing
        check_out_date: Check-out date for pricing
        is_corporate: Is this a corporate booking? (affects pricing)
    
    Returns:
        JSON string with complete hotel details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM hotels WHERE hotel_id = %s", (hotel_id,))
        hotel = cursor.fetchone()
        if not hotel:
            raise ValueError("Hotel not found")
        
        hotel_dict = dict(hotel)
        
        cursor.execute("""
            SELECT r.*, MIN(ri.available_count) as min_availability
            FROM rooms r
            LEFT JOIN room_inventory ri ON r.room_id = ri.room_id
            WHERE r.hotel_id = %s
                AND (ri.date >= %s AND ri.date < %s)
            GROUP BY r.room_id, r.hotel_id, r.room_type, r.base_price, 
                     r.max_occupancy, r.bed_type, r.amenities
            ORDER BY r.base_price ASC
        """, (hotel_id, check_in_date, check_out_date))
        
        rooms = cursor.fetchall()
        rooms_with_pricing = []
        
        for room in rooms:
            room_dict = dict(room)
            
            pricing = calculate_dynamic_price(
                float(room["base_price"]),
                check_in_date,
                check_out_date,
                hotel_id,
                is_corporate,
                float(hotel["corporate_discount_percent"])
            )
            
            room_dict.update({
                "calculated_total": pricing["total_price"],
                "average_per_night": pricing["average_per_night"],
                "nights": pricing["nights"],
                "pricing_breakdown": pricing["breakdown"],
                "available_rooms": room["min_availability"] or 0
            })
            
            # Convert Decimal to float
            room_dict["base_price"] = float(room_dict["base_price"])
            
            rooms_with_pricing.append(room_dict)
        
        cursor.execute("""
            SELECT amenity_name, amenity_type
            FROM hotel_amenities
            WHERE hotel_id = %s
        """, (hotel_id,))
        
        amenities = [dict(row) for row in cursor.fetchall()]
        
        # Convert Decimal fields to float
        hotel_dict["corporate_discount_percent"] = float(hotel_dict.get("corporate_discount_percent", 0))
        
        hotel_dict["rooms"] = rooms_with_pricing
        hotel_dict["amenities"] = amenities
        hotel_dict["dates"] = {
            "check_in": check_in_date,
            "check_out": check_out_date
        }
        hotel_dict["pricing_is_corporate"] = is_corporate
    
    return json.dumps(hotel_dict, indent=2, default=str)


@mcp.tool
def create_booking(
    hotel_id: int,
    room_id: int,
    check_in_date: str,
    check_out_date: str,
    guest_name: str,
    guest_email: str,
    is_corporate: bool = False,
    company_name: Optional[str] = None,
    guest_count: int = 1,
    purpose_of_travel: Optional[str] = None
) -> str:
    """Create a new hotel booking. No pre-registration needed!
    
    Args:
        hotel_id: Hotel ID
        room_id: Room ID to book
        check_in_date: Check-in date (YYYY-MM-DD)
        check_out_date: Check-out date (YYYY-MM-DD)
        guest_name: Guest's full name (first and last name optional)
        guest_email: Guest's email address
        is_corporate: Is this a corporate booking?
        company_name: Company name (if corporate booking)
        guest_count: Number of guests (default 1)
        purpose_of_travel: Purpose of the trip
    
    Returns:
        JSON string with booking confirmation
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Get or create user
            cursor.execute("""
                SELECT user_id FROM users WHERE LOWER(email) = LOWER(%s)
            """, (guest_email,))
            
            user = cursor.fetchone()
            
            if user:
                user_id = user["user_id"]
            else:
                # Create new user record
                name_parts = guest_name.strip().split(' ', 1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ""
                
                # Generate user code
                user_code = f"{'CORP' if is_corporate else 'INDV'}{random.randint(1000, 9999)}"
                
                cursor.execute("""
                    INSERT INTO users (
                        user_code, first_name, last_name, email,
                        is_corporate, company_name
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING user_id
                """, (user_code, first_name, last_name, guest_email,
                      is_corporate, company_name))
                
                user_id = cursor.fetchone()["user_id"]
            
            # Get hotel and room info
            cursor.execute("""
                SELECT h.corporate_discount_percent, r.base_price
                FROM hotels h
                JOIN rooms r ON h.hotel_id = r.hotel_id
                WHERE h.hotel_id = %s AND r.room_id = %s
            """, (hotel_id, room_id))
            
            hotel_room = cursor.fetchone()
            if not hotel_room:
                raise ValueError("Hotel or room not found")
            
            # Check availability
            cursor.execute("""
                SELECT MIN(available_count) as min_avail
                FROM room_inventory
                WHERE room_id = %s
                    AND date >= %s
                    AND date < %s
            """, (room_id, check_in_date, check_out_date))
            
            avail = cursor.fetchone()
            if not avail or avail["min_avail"] < 1:
                raise ValueError("Room not available for selected dates")
            
            # Calculate pricing
            pricing = calculate_dynamic_price(
                float(hotel_room["base_price"]),
                check_in_date,
                check_out_date,
                hotel_id,
                is_corporate,
                float(hotel_room["corporate_discount_percent"])
            )
            
            # Generate booking reference
            booking_ref = generate_booking_reference()
            
            # Create booking
            cursor.execute("""
                INSERT INTO bookings (
                    booking_reference, user_id, hotel_id, room_id,
                    check_in_date, check_out_date, nights,
                    guest_name, guest_count, total_amount, per_night_rate,
                    corporate_discount, status, purpose_of_travel
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                booking_ref, user_id, hotel_id, room_id,
                check_in_date, check_out_date, pricing["nights"],
                guest_name, guest_count, pricing["total_price"],
                pricing["average_per_night"],
                pricing["corporate_discount_applied"],
                "confirmed", purpose_of_travel or ""
            ))
            
            # Update inventory
            cursor.execute("""
                UPDATE room_inventory
                SET available_count = available_count - 1
                WHERE room_id = %s
                    AND date >= %s
                    AND date < %s
            """, (room_id, check_in_date, check_out_date))
            
            conn.commit()
            
            return json.dumps({
                "success": True,
                "booking_reference": booking_ref,
                "status": "confirmed",
                "details": {
                    "guest_name": guest_name,
                    "guest_email": guest_email,
                    "is_corporate": is_corporate,
                    "company": company_name,
                    "check_in": check_in_date,
                    "check_out": check_out_date,
                    "nights": pricing["nights"],
                    "total_amount": pricing["total_price"],
                    "per_night_rate": pricing["average_per_night"],
                    "corporate_discount": pricing["corporate_discount_applied"],
                    "purpose": purpose_of_travel or ""
                },
                "message": "Booking confirmed successfully"
            }, indent=2)
        
        except Exception as e:
            conn.rollback()
            raise


@mcp.tool
def check_room_availability(
    room_id: int,
    check_in_date: str,
    check_out_date: str
) -> str:
    """Check real-time availability for specific room.
    
    Args:
        room_id: Room ID
        check_in_date: Check-in date
        check_out_date: Check-out date
    
    Returns:
        JSON string with availability details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT r.*, h.hotel_name, h.city
            FROM rooms r
            JOIN hotels h ON r.hotel_id = h.hotel_id
            WHERE r.room_id = %s
        """, (room_id,))
        
        room = cursor.fetchone()
        if not room:
            raise ValueError("Room not found")
        
        cursor.execute("""
            SELECT date, available_count, price
            FROM room_inventory
            WHERE room_id = %s
                AND date >= %s
                AND date < %s
            ORDER BY date ASC
        """, (room_id, check_in_date, check_out_date))
        
        inventory = cursor.fetchall()
        
        if not inventory:
            return json.dumps({
                "available": False,
                "message": "No inventory data for the selected dates",
                "room_info": dict(room)
            }, indent=2, default=str)
        
        min_availability = min(row["available_count"] for row in inventory)
        
        # Convert inventory data
        inventory_list = []
        for row in inventory:
            inv_dict = dict(row)
            inv_dict["price"] = float(inv_dict["price"])
            inventory_list.append(inv_dict)
        
        room_dict = dict(room)
        room_dict["base_price"] = float(room_dict["base_price"])
        
        return json.dumps({
            "available": min_availability > 0,
            "room_id": room_id,
            "hotel_name": room_dict["hotel_name"],
            "city": room_dict["city"],
            "room_type": room_dict["room_type"],
            "max_occupancy": room_dict["max_occupancy"],
            "available_rooms": min_availability,
            "daily_availability": inventory_list
        }, indent=2, default=str)


@mcp.tool
def get_booking_details(booking_reference: str) -> str:
    """Get complete booking details by reference number.
    
    Args:
        booking_reference: Booking reference number
    
    Returns:
        JSON string with complete booking details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                b.*,
                u.first_name || ' ' || u.last_name as user_name,
                u.email as user_email,
                u.is_corporate,
                u.company_name,
                h.hotel_name,
                h.address,
                h.city,
                h.state,
                h.phone as hotel_phone,
                r.room_type,
                r.bed_type
            FROM bookings b
            JOIN users u ON b.user_id = u.user_id
            JOIN hotels h ON b.hotel_id = h.hotel_id
            JOIN rooms r ON b.room_id = r.room_id
            WHERE b.booking_reference = %s
        """, (booking_reference,))
        
        booking = cursor.fetchone()
        if not booking:
            raise ValueError("Booking not found")
        
        booking_dict = dict(booking)
        # Convert Decimal fields to float
        booking_dict["total_amount"] = float(booking_dict["total_amount"])
        booking_dict["per_night_rate"] = float(booking_dict["per_night_rate"])
        booking_dict["corporate_discount"] = float(booking_dict["corporate_discount"])
        
        return json.dumps(booking_dict, indent=2, default=str)


@mcp.tool
def list_bookings_by_email(
    guest_email: str,
    status: Optional[str] = None,
    include_past: bool = False
) -> str:
    """List all bookings for a guest by their email.
    
    Args:
        guest_email: Guest's email address
        status: Filter by status (pending, confirmed, completed, cancelled)
        include_past: Include past bookings (default false)
    
    Returns:
        JSON string with list of bookings
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get user
        cursor.execute("""
            SELECT user_id, first_name, last_name, is_corporate, company_name
            FROM users WHERE LOWER(email) = LOWER(%s)
        """, (guest_email,))
        
        user = cursor.fetchone()
        if not user:
            return json.dumps({
                "message": "No bookings found for this email address",
                "bookings_count": 0,
                "bookings": []
            }, indent=2)
        
        user_id = user["user_id"]
        
        query = """
            SELECT 
                b.booking_reference,
                b.status,
                h.hotel_name,
                h.city,
                r.room_type,
                b.check_in_date,
                b.check_out_date,
                b.nights,
                b.total_amount,
                b.purpose_of_travel
            FROM bookings b
            JOIN hotels h ON b.hotel_id = h.hotel_id
            JOIN rooms r ON b.room_id = r.room_id
            WHERE b.user_id = %s
        """
        params = [user_id]
        
        if status:
            query += " AND b.status = %s"
            params.append(status)
        
        if not include_past:
            query += " AND b.check_out_date >= CURRENT_DATE"
        
        query += " ORDER BY b.check_in_date DESC"
        
        cursor.execute(query, params)
        bookings = cursor.fetchall()
        
        # Convert bookings
        bookings_list = []
        for booking in bookings:
            booking_dict = dict(booking)
            booking_dict["total_amount"] = float(booking_dict["total_amount"])
            bookings_list.append(booking_dict)
        
        return json.dumps({
            "guest_name": f"{user['first_name']} {user['last_name']}",
            "guest_email": guest_email,
            "is_corporate": bool(user["is_corporate"]),
            "company": user["company_name"],
            "bookings_count": len(bookings_list),
            "bookings": bookings_list
        }, indent=2, default=str)


@mcp.tool
def cancel_booking(
    booking_reference: str,
    guest_email: str
) -> str:
    """Cancel an existing booking and restore inventory.
    
    Args:
        booking_reference: Booking reference to cancel
        guest_email: Guest's email address (for verification)
    
    Returns:
        JSON string with cancellation confirmation
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Get user
            cursor.execute("""
                SELECT user_id FROM users WHERE LOWER(email) = LOWER(%s)
            """, (guest_email,))
            
            user = cursor.fetchone()
            if not user:
                raise ValueError("Guest not found")
            
            user_id = user["user_id"]
            
            # Get booking
            cursor.execute("""
                SELECT user_id, room_id, check_in_date, check_out_date, status
                FROM bookings
                WHERE booking_reference = %s
            """, (booking_reference,))
            
            booking = cursor.fetchone()
            if not booking:
                raise ValueError("Booking not found")
            
            if booking["user_id"] != user_id:
                raise ValueError("Unauthorized: Booking does not belong to this guest")
            
            if booking["status"] == "cancelled":
                raise ValueError("Booking is already cancelled")
            
            # Update booking status
            cursor.execute("""
                UPDATE bookings
                SET status = 'cancelled'
                WHERE booking_reference = %s
            """, (booking_reference,))
            
            # Restore inventory
            cursor.execute("""
                UPDATE room_inventory
                SET available_count = available_count + 1
                WHERE room_id = %s
                    AND date >= %s
                    AND date < %s
            """, (booking["room_id"], booking["check_in_date"], booking["check_out_date"]))
            
            conn.commit()
            
            return json.dumps({
                "success": True,
                "booking_reference": booking_reference,
                "status": "cancelled",
                "message": "Booking cancelled successfully"
            }, indent=2)
        
        except Exception as e:
            conn.rollback()
            raise


@mcp.tool
def calculate_trip_cost(
    hotel_id: int,
    room_id: int,
    check_in_date: str,
    check_out_date: str,
    is_corporate: bool = False
) -> str:
    """Calculate estimated trip cost with detailed breakdown.
    
    Args:
        hotel_id: Hotel ID
        room_id: Room ID
        check_in_date: Check-in date
        check_out_date: Check-out date
        is_corporate: Is this a corporate booking?
    
    Returns:
        JSON string with detailed cost breakdown
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT h.hotel_name, h.city, h.corporate_discount_percent,
                   r.room_type, r.base_price
            FROM hotels h
            JOIN rooms r ON h.hotel_id = r.hotel_id
            WHERE h.hotel_id = %s AND r.room_id = %s
        """, (hotel_id, room_id))
        
        info = cursor.fetchone()
        if not info:
            raise ValueError("Hotel or room not found")
        
        pricing = calculate_dynamic_price(
            float(info["base_price"]),
            check_in_date,
            check_out_date,
            hotel_id,
            is_corporate,
            float(info["corporate_discount_percent"])
        )
        
        return json.dumps({
            "hotel": {
                "name": info["hotel_name"],
                "city": info["city"],
                "room_type": info["room_type"]
            },
            "dates": {
                "check_in": check_in_date,
                "check_out": check_out_date,
                "nights": pricing["nights"]
            },
            "cost_breakdown": {
                "base_price_per_night": float(info["base_price"]),
                "total_base_price": float(info["base_price"]) * pricing["nights"],
                "after_dynamic_pricing": pricing["breakdown"][0]["after_dynamic_pricing"],
                "corporate_discount_percent": float(info["corporate_discount_percent"]) if is_corporate else 0,
                "corporate_discount_amount": pricing["corporate_discount_applied"],
                "final_total": pricing["total_price"],
                "average_per_night": pricing["average_per_night"]
            },
            "is_corporate_booking": is_corporate
        }, indent=2)


@mcp.tool
def get_cities() -> str:
    """Get list of all cities with available hotels.
    
    Returns:
        JSON string with list of cities
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT city, state, country, COUNT(hotel_id) as hotel_count
            FROM hotels
            GROUP BY city, state, country
            ORDER BY city
        """)
        
        cities = [dict(row) for row in cursor.fetchall()]
        
        return json.dumps({
            "cities_count": len(cities),
            "cities": cities
        }, indent=2)


@mcp.tool
def get_hotel_amenities(hotel_id: int) -> str:
    """Get all amenities for a specific hotel.
    
    Args:
        hotel_id: Hotel ID
    
    Returns:
        JSON string with hotel amenities
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT h.hotel_name, h.city
            FROM hotels h
            WHERE h.hotel_id = %s
        """, (hotel_id,))
        
        hotel = cursor.fetchone()
        if not hotel:
            raise ValueError("Hotel not found")
        
        cursor.execute("""
            SELECT amenity_name, amenity_type
            FROM hotel_amenities
            WHERE hotel_id = %s
            ORDER BY amenity_type, amenity_name
        """, (hotel_id,))
        
        amenities = [dict(row) for row in cursor.fetchall()]
        
        return json.dumps({
            "hotel_id": hotel_id,
            "hotel_name": hotel["hotel_name"],
            "city": hotel["city"],
            "amenities_count": len(amenities),
            "amenities": amenities
        }, indent=2)


@mcp.tool
def get_preferred_vendors(city: Optional[str] = None) -> str:
    """Get list of preferred vendor hotels.
    
    Args:
        city: Optional city filter
    
    Returns:
        JSON string with list of preferred vendors
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT hotel_id, hotel_name, hotel_code, chain, star_rating,
                   city, state, country, corporate_discount_percent
            FROM hotels
            WHERE is_preferred_vendor = TRUE
        """
        params = []
        
        if city:
            query += " AND LOWER(city) = LOWER(%s)"
            params.append(city)
        
        query += " ORDER BY city, hotel_name"
        
        cursor.execute(query, params)
        hotels = cursor.fetchall()
        
        hotels_list = []
        for hotel in hotels:
            hotel_dict = dict(hotel)
            hotel_dict["corporate_discount_percent"] = float(hotel_dict["corporate_discount_percent"])
            hotels_list.append(hotel_dict)
        
        return json.dumps({
            "preferred_vendors_count": len(hotels_list),
            "hotels": hotels_list
        }, indent=2)


@mcp.tool
def get_corporate_bookings(company_name: str) -> str:
    """Get all bookings for a specific company.
    
    Args:
        company_name: Company name
    
    Returns:
        JSON string with company bookings
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                b.booking_reference,
                b.status,
                u.first_name || ' ' || u.last_name as traveler_name,
                u.email,
                h.hotel_name,
                h.city,
                r.room_type,
                b.check_in_date,
                b.check_out_date,
                b.nights,
                b.total_amount,
                b.corporate_discount,
                b.purpose_of_travel
            FROM bookings b
            JOIN users u ON b.user_id = u.user_id
            JOIN hotels h ON b.hotel_id = h.hotel_id
            JOIN rooms r ON b.room_id = r.room_id
            WHERE u.company_name = %s
            ORDER BY b.check_in_date DESC
        """, (company_name,))
        
        bookings = cursor.fetchall()
        
        bookings_list = []
        total_spent = 0
        total_saved = 0
        
        for booking in bookings:
            booking_dict = dict(booking)
            booking_dict["total_amount"] = float(booking_dict["total_amount"])
            booking_dict["corporate_discount"] = float(booking_dict["corporate_discount"])
            total_spent += booking_dict["total_amount"]
            total_saved += booking_dict["corporate_discount"]
            bookings_list.append(booking_dict)
        
        return json.dumps({
            "company_name": company_name,
            "bookings_count": len(bookings_list),
            "total_spent": round(total_spent, 2),
            "total_saved": round(total_saved, 2),
            "bookings": bookings_list
        }, indent=2)


# ============================================================================
# MAIN SERVER ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    logger.info(f"Starting Hotel Booking FastMCP Server with Neon PostgreSQL database")
    
    try:
        # Test database connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM hotels")
            result = cursor.fetchone()
            logger.info(f"Database connected successfully. Hotels in database: {result['count']}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        exit(1)
    
    mcp.run(transport="http")