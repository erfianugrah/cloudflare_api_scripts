import os
import asyncio
import aiohttp
import json
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://api.cloudflare.com/client/v4/zones"
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}

async def fetch_data(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            raise Exception(f"Failed to retrieve data. Status code: {response.status}")
        return await response.json()

async def get_zone_data(session):
    zone_data = []
    page = 1
    while True:
        url = f"{BASE_URL}?page={page}&per_page=1000"
        data = await fetch_data(session, url)
        zone_data.extend([{"id": zone["id"], "name": zone["name"]} for zone in data["result"]])
        if len(data["result"]) < 1000:
            break
        page += 1
    return zone_data

async def get_custom_hostnames(session, zone):
    custom_hostnames = []
    page = 1
    while True:
        url = f"{BASE_URL}/{zone['id']}/custom_hostnames?page={page}&per_page=1000"
        data = await fetch_data(session, url)
        for hostname in data["result"]:
            custom_hostname = {
                "id": hostname["id"],
                "hostname": hostname["hostname"],
                "status": hostname["status"],
                "custom_origin_server": hostname.get("custom_origin_server", "null"),
                "custom_origin_sni": hostname.get("custom_origin_sni", "null"),
                "ssl_status": hostname["ssl"]["status"],
                "ssl_method": hostname["ssl"]["method"],
                "ssl_type": hostname["ssl"]["type"],
                "created_at": hostname["created_at"],
                "zone_id": zone["id"],
                "zone_name": zone["name"]
            }
            if "verification_errors" in hostname:
                custom_hostname["verification_errors"] = hostname["verification_errors"]
            custom_hostnames.append(custom_hostname)
        if len(data["result"]) < 1000:
            break
        page += 1
    return custom_hostnames

async def main():
    async with aiohttp.ClientSession(headers=headers) as session:
        zone_data = await get_zone_data(session)
        tasks = [get_custom_hostnames(session, zone) for zone in zone_data]
        all_custom_hostnames = await asyncio.gather(*tasks)
    
    flattened_hostnames = [item for sublist in all_custom_hostnames for item in sublist]
    
    output = {
        "total_count": len(flattened_hostnames),
        "custom_hostnames": flattened_hostnames
    }
    
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
