import requests

response = requests.get("https://jsonplaceholder.typicode.com/posts/1")
print(response.text)   # Raw JSON string

# data = response.json() # Parsed into Python dict
# print(data["title"])   # Access a specific field