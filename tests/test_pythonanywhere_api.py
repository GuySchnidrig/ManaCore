import requests
from datetime import datetime, timedelta

username = 'GuySchnidrig'
token = ''
headers = {"Authorization": f"Token {token}"}


# Schedule the task to run about 1 minute from now (UTC)
now = datetime.utcnow()
next_minute = now + timedelta(minutes=1)
hour = next_minute.hour
minute = next_minute.minute

# Define updated parameters
data = {
    "command": "python3.10 /home/GuySchnidrig/update_data.py",
    "enabled": True,
    "description": "Run update_data.py shortly (updated)",
    "interval": "daily",      # required field
    "hour": hour,             # next minute
    "minute": minute
}

# PUT request to update existing scheduled task by ID
r = requests.put(
    f"https://www.pythonanywhere.com/api/v0/user/{username}/schedule/1262825/",
    headers=headers,
    json=data
)

print("Status:", r.status_code)
print("Response:", r.text)