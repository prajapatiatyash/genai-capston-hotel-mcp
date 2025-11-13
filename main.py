# main.py

from fastapi import FastAPI
# Import the mcp instance from your other file
from hotel_booking_mcp_server import mcp

# Create the main FastAPI application
app = FastAPI(
    title="Airline Booking API",
    description="A FastAPI server that hosts the Airline Booking FastMCP toolset.",
    version="1.0.0",
)

# This is a standard API endpoint, useful for health checks
@app.get("/", tags=["Status"])
async def read_root():
    """
    Root endpoint to check if the API is running.
    """
    return {
        "status": "ok",
        "message": "Welcome to the Airline Booking API.",
        "mcp_tools_path": "/mcp"
    }

# Mount the FastMCP application at the /mcp path
# All requests to /mcp/* will be handled by your tool server
app.mount("/mcp", mcp.http_app())

# To run this application:
# 1. Install dependencies: pip install -r requirements.txt
# 2. Start the server: uvicorn main:app --reload