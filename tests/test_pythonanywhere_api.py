import requests
username = 'GuySchnidrig'
token = 'XXX'

# response
response = requests.get(
    'https://www.pythonanywhere.com/api/v0/user/{username}/cpu/'.format(
        username=username
    ),
    headers={'Authorization': 'Token {token}'.format(token=token)}
)
if response.status_code == 200:
    print('CPU quota info:')
    print(response.content)
else:
    print('Got unexpected status code {}: {!r}'.format(response.status_code, response.content))

# tasks
url = f"https://www.pythonanywhere.com/api/v0/user/{username}/schedule/"
headers = {"Authorization": f"Token {token}"}
response = requests.get(url, headers=headers)
print("Status code:", response.status_code)
print("Response headers:", response.headers)
print("Raw text:", response.text)  # 👈 check what it actually returns

if response.status_code == 200:
    data = response.json()
    print(type(data))  # should be <class 'list'>
    print(data)
else:
    print("Error:", response.text)