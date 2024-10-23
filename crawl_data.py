import os
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json

prices = ['gia-duoi-1-trieu',
            'gia-1-trieu-den-3-trieu',
            'gia-3-trieu-den-5-trieu',
            'gia-5-trieu-den-10-trieu',
            'gia-10-trieu-den-40-trieu',
            'gia-40-trieu-den-70-trieu',
            'gia-70-trieu-den-100-trieu',
            'gia-tren-100-trieu']

areas = ['dt-duoi-30m',
            'dt-tu-30m2-den-50m2',
            'dt-tu-50m2-den-80m2',
            'dt-tu-80m2-den-100m2',
            'dt-tu-100m2-den-150m2',
            'dt-tu-150m2-den-200m2',
            'dt-tu-200m2-den-250m2',
            'dt-tu-250m2-den-300m2',
            'dt-tu-300m2-den-500m2',
            'dt-tren-500m2']

contents = ['cho-thue-nha-tro-phong-tro',
           'cho-thue-can-ho-chung-cu',
           'cho-thue-nha-rieng'
           ]
# Mark the start time
start_time = time.time()
# Define the number of pages and columns for the DataFrame
n_pages = 100
cols = ['Header','Diện tích', 'Mức giá', 'Hướng nhà', 'Số phòng ngủ', 'Số toilet', 'Số tầng', 'Pháp lý', 'Đường vào']
df = pd.DataFrame({key: [] for key in cols})
df.to_csv('data.csv', mode='a', header=True, index=False)

# Function to scrape data for a single room
def scrape_room(room_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
    }
    data_frame = {key: [] for key in cols}

    try:
        room_response = requests.get('https://muonnha.com.vn/' + room_url, headers=headers)
        if room_response.status_code == 200:
            room_soup = bs(room_response.content, 'html.parser')

            room_header = room_soup.find('h1', class_='mx-2 text-justify font-lexend text-xl lg:mx-0 lg:text-3xl leading-8 capitalize')
            data_frame['Header'].append(room_header.text)
            # Get the room details
            attributes_list = room_soup.find_all('div', class_='flex h-[46px] items-center border-b border-greyf2f2f2')
            for attribute in attributes_list:
                attributes = attribute.find_all('span', class_='flex-1')
                key = attributes[0].text.strip()
                value = attributes[1].text.strip()
                
                if key in cols:
                    data_frame[key].append(value)

            # Fill missing data with 'None'
            for key in cols:
                if len(data_frame[key]) == 0:
                    data_frame[key].append(None)

            # Write data to CSV
            df = pd.DataFrame(data_frame)
            df.to_csv('data.csv', mode='a', header=False, index=False)

    except Exception as e:
        print(f"Error scraping room {room_url}: {e}")

# Function to scrape data for a single page
def scrape_page( content,page_idx, price, area):
    url = f'https://muonnha.com.vn/{content}/{price}-{area}/p{page_idx+1}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = bs(response.content, 'html.parser')
            room_list = soup.find_all(
                'a',
                class_='relative mb-2 inline-block w-full overflow-hidden border-t border-greyf2f2f2 sm:mb-4 sm:rounded sm:border sm:shadow-item sm:hover:shadow-item_hover'
            )
            res = []
            for room in room_list:
                prev_sibling = room.find_previous_sibling()
                if (prev_sibling and prev_sibling.name == 'h2'):
                    break
                room_url = room['href']
                res.append(room_url)
                next_sibling = room.find_next_sibling()
                if (next_sibling and next_sibling.name == 'h2'):
                    break
            return res
        else:
            print(f"Error: {response.status_code} on page {page_idx}")
            return []
    except Exception as e:
        print(f"Error fetching page {page_idx}: {e}")
        return []

# Use ThreadPoolExecutor to scrape multiple pages and rooms concurrently
with ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(scrape_page,content, page_idx, price, area)for content in contents for page_idx in range(n_pages) for price in prices for area in areas]
    
    # Collect all room URLs
    all_room_urls = []
    for future in as_completed(futures):
        all_room_urls.extend(future.result())

    # Scrape room details concurrently
    room_futures = [executor.submit(scrape_room, room_url) for room_url in all_room_urls]
    for future in as_completed(room_futures):
        future.result()  # Ensure all tasks complete

end = time.time()
print(f"Time: {end - start_time}")
