
import requests

def get_open_brewery_api(url, page, items_per_page):

    response = requests.get(url + f'?page={page}&per_page={items_per_page}')

    if response.status_code == 200:
        breweries_data = response.json()
    else:
        print("API Error:", response.status_code)

    return breweries_data

import requests

def get_max_page(url, items_per_page):

    def page_exists(page):
        response = requests.get(f"{url}?page={page}&per_page={items_per_page}")
        if response.status_code != 200:
            raise Exception(f"Request Error: {response.status_code}")

        data = response.json()
        return len(data) > 0

    low = 1
    high = 2

    while page_exists(high):
        low = high
        high *= 2

    while low < high:
        mid = (low + high + 1) // 2
        if page_exists(mid):
            low = mid
        else:
            high = mid - 1

    return low