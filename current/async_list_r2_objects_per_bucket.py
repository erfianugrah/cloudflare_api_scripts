import os
import asyncio
import aiohttp
import json

BASE_URL = "https://api.cloudflare.com/client/v4/accounts"
account_id = os.environ.get('CLOUDFLARE_ACCOUNT_ID')
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

headers = {
    key: value for key, value in {
        "X-Auth-Key": auth_key,
        "X-Auth-Email": auth_email
    }.items() if value is not None
}

def format_size(size_bytes):
    # Convert bytes to MB, GB, or TB as appropriate
    size_mb = size_bytes / (1024 * 1024)
    if size_mb < 1024:
        return f"{size_mb:.2f} MB"
    size_gb = size_mb / 1024
    if size_gb < 1024:
        return f"{size_gb:.2f} GB"
    size_tb = size_gb / 1024
    return f"{size_tb:.2f} TB"

async def fetch_data(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            error_text = await response.text()
            print(f"URL: {url}")
            print(f"Headers: {headers}")
            print(f"Response status: {response.status}")
            print(f"Response body: {error_text}")
            raise Exception(f"Failed to retrieve data. Status code: {response.status}")
        return await response.json()

async def get_r2_objects(session, bucket_name):
    r2_objects = []
    total_size = 0
    cursor = None
    while True:
        url = f"{BASE_URL}/{account_id}/r2/buckets/{bucket_name}/objects?per_page=1000&delimiter=/"
        if cursor:
            url += f"&cursor={cursor}"
        data = await fetch_data(session, url)
        
        if not data.get("success", False):
            raise Exception("API request failed")
        
        for obj in data.get("result", []):
            r2_objects.append({
                "name": obj["key"],
                "size": obj["size"],
                "content_type": obj["http_metadata"].get("contentType", "application/octet-stream")
            })
            total_size += obj["size"]
        
        # Check if there are more pages
        if "cursor" not in data:
            break
        cursor = data["cursor"]
    
    return r2_objects, total_size

async def main():
    bucket_name = input("Enter the bucket name: ")
    
    async with aiohttp.ClientSession(headers=headers) as session:
        r2_objects, total_size = await get_r2_objects(session, bucket_name)
    
    output = {
        "total_count": len(r2_objects),
        "total_size": format_size(total_size),
        "total_size_bytes": total_size,
        "objects": r2_objects
    }
    
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
