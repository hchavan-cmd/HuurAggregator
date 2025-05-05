from src.utils import ScraperUtils
import os
from bs4 import BeautifulSoup
import time
import random

from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# import undetected_chromedriver as uc ## use py 3.11 or lesser for this to work.


import json
import re
import pandas as pd
import numpy as np


class FundaScraper(ScraperUtils):
    def __init__(self, output_path:str):
        super().__init__()
        self.input_dict = dict()
        self.processing_listing_urls = []
        self.num_pages = 1 ## initial value set to one
        self.homepage_url = str()
        self.output_file = output_path
        self.saved_df = pd.DataFrame()
        self.new_listings_df = pd.DataFrame(columns=['price', 'address', 'city', 'province', 'postcode', 'living_area', 'num_bedrooms', 'kenmerken', 'omschrijving', 'phone', 'makelaar_name', 'makelaar_url', 'status', 'listing_url', 'timestamp'])

        pass

    def create_input_filters(self):
        self.input_dict["selected_area"] = ["utrecht", "rotterdam", "ijsselstein"]
        self.input_dict["price"] = "600-1400"
        self.input_dict["object_type"] = ["house", "apartment"] #["house","apartment","parking","land","storage_space","storage","berth","substructure","pitch"]
        # self.input_dict["publication_date"]= "5"
        self.input_dict['availability']=["available"] #"unavailable"
        # self.input_dict["floor_area"] = "30-"
        # self.input_dict["plot_area"]="30-"
        # self.input_dict["rooms"]="1-"
        # self.input_dict["bedrooms"] = "1-"
        # self.input_dict["bathrooms"]="1-"
        # self.input_dict["rental_agreement"] = ["indefinite_duration", "temporary_rent"]
        # self.input_dict["renting_condition"] = ["furnished", "partially_finished", "service_cost_included", "service_cost_excluded"]
        # self.input_dict["construction_type"] = ["newly_built","resale"]
        # self.input_dict["open_house"] = ["all","coming_weekend","today"]

        return self.filters_to_string()
    
    def filters_to_string(self):
        all_filters_stringified = list()
        for i in self.input_dict.keys():
            if isinstance(self.input_dict[i], str):
                all_filters_stringified.append(f'{i}="{self.input_dict[i]}"')
            elif isinstance(self.input_dict[i], list):
                # Convert list items to strings with double quotes
                quoted_items = [f'"{item}"' for item in self.input_dict[i]]
                all_filters_stringified.append(f"{i}=[{','.join(quoted_items)}]")

        return all_filters_stringified
    
    def decline_cookie_button_click(self):
        decline_button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="didomi-notice-disagree-button"]'))
        )

        # Click the decline button
        decline_button.click()
        self.human_sleep()  # Simulate delay after clicking the decline button

    def landing_page(self):
        all_filters = self.create_input_filters()
        # URL of homepage
        self.homepage_url = "https://www.funda.nl/zoeken/huur"+"?"+"&".join(all_filters)

        self.driver.get(self.homepage_url)

        try:
            self.decline_cookie_button_click()
        except Exception as e:
            print(f"Error while clicking decline button: {e}")

        html_source = self.driver.page_source
        soup_search_results = BeautifulSoup(html_source, 'html.parser')

        ### all <a> tag relating to page numbers. 
        pages = soup_search_results.find_all('a', href=re.compile(r'\?page=\d+'))
        self.num_pages = max(max([int(re.search(r'(\d+)$', i['href']).group(1)) for i in pages]), 1) ## from list of page numbers scrapted, pick max
        return soup_search_results

    @staticmethod
    def parse_search_results_json(soup_search_results):
        scraped_listings = []
        script_tag = soup_search_results.find('script', {'type': 'application/ld+json'}) ## on the homepage/listings page, find the element containing urls linking to all listing.
        if script_tag:
            json_data = json.loads(script_tag.string)
            # Extract URLs from the itemListElement
            scraped_listings.extend([item['url'] for item in json_data['itemListElement']])
            
            # Print out the extracted URLs
            for url in scraped_listings:
                print(url)
        else:
            print("No script tag with JSON-LD data found.")
        return scraped_listings

    def create_list_of_listings(self):
        soup_search_results = self.landing_page() ## scraping first search results page to find listings and also find number of pages. 
        listing_urls = []
        for i in np.arange(1, self.num_pages + 1):
            if i > 1:
                self.human_sleep()
                soup_search_results = self.retrieve_html(self.homepage_url + "&search_result="+str(i), self.driver)

            listing_urls.extend(self.parse_search_results_json(soup_search_results))

        return listing_urls

    def get_unprocessed_listings(self):
        listing_urls = self.create_list_of_listings()
        for url in listing_urls:
            if os.path.exists(self.output_file):
                    self.saved_df = pd.read_parquet(self.output_file)
                    if url in self.saved_df['listing_url'].values:
                        print('Already scraped. Ignore or repeat scrape later.')
                    else:
                        print('New listing. Processing:')
                        self.processing_listing_urls.append(url)

            else:
                print('No output file found.')
                self.processing_listing_urls.append(url)

    def scrape_listing(self, url:str):
        # output_list = [None] * len(self.new_listings_df.columns)
        out = dict()
        scrape_time = time.time()
        out['timestamp'] = scrape_time
        
        soup = self.retrieve_html(url, self.driver)

        out['price'] = self.retrieve_price(soup) or None
        out['omschrijving'] = self.retrieve_omschrijving(soup) or None
        out['address'] = self.retrieve_address_title(soup) or None
        out['city'], out['province'], out['postcode'] = self.retrieve_location(soup)
        out['num_bedrooms'] = self.get_value_preceding_text(soup, 'slaapkamers')
        out['living_area'], out['kenmerken'], out['status'] = self.retrieve_kenmerken(soup) 
        out['listing_url'] = url
        out.update(self.retrieve_makelaar_details(soup))
        out['phone'] = self.retrieve_phone(soup) or None
        # self.new_listings_df.loc[len(self.new_listings_df)] = out
        return out

    @staticmethod
    def retrieve_price(soup):
        ld_json_script = soup.find('script', type='application/ld+json')
        price = json.loads(ld_json_script.string)['offers']['price']

        return price 

    @staticmethod
    def retrieve_omschrijving(soup):
        # Find the <h2> element with text "Omschrijving" and find the next div with class 'listing-description-text'
        h2_element = soup.find('h2', string="Omschrijving")

        # Check if we found the h2 element
        if h2_element:
            # Find the next div element after the h2
            description_div = h2_element.find_next('div', class_='listing-description-text')
            
            # Extract and print the text inside the description div
            if description_div:
                omschrijving = description_div.get_text(strip=True)
                # print(omschrijving)
                print('Omschrijving found: '+ omschrijving[:100])
                return omschrijving
            else:
                print("Description div not found.")
        else:
            print("Heading 'Omschrijving' not found.")
        return None
    
    @staticmethod
    def retrieve_address_title(soup):
        ## extract title of page (address)

        title_text = soup.title.get_text()

        # Step 2: Use regex to extract text after colon and before "[Funda]" (if present)
        match = re.search(r':\s*(.*?)\s*(?:\[Funda\])?$', title_text)
        if match:
            address = match.group(1)
            print('Address: '+ address)  # Output: Hoekeindseweg 162 2665 KH Bleiswijk
        else:
            print("No address match found in title.")
        
        return address
    
    @staticmethod
    def retrieve_location(soup):
        city_divs = soup.find_all('div', attrs={"city": True}) ## contains all info about the house address
        city = city_divs[0]['city']
        province = city_divs[0]['province']
        house_number = city_divs[0]['housenumber']
        neighborhood = city_divs[0]['neighborhoodidentifier']
        postcode = city_divs[0]['postcode']

        # out = [city, province, postcode]
        return city, province, postcode
    
    @staticmethod
    def get_value_preceding_text(soup, text_search:str, element: str = "span"):
        # Find the span that contains text_search
        wonen_span = soup.find(element, string=lambda t: t and text_search in t.lower())

        # Get the previous sibling elementt
        if wonen_span:
            prev_span = wonen_span.find_previous_sibling(element)
            if prev_span:
                return(prev_span.text.strip())
            
    @staticmethod       
    def kenmerken_extract(sec_kenmerken): ## extract kenmerken table funda
        kenmerken_names = [i.text.strip() for i in sec_kenmerken.find_all("dt")]
        kenmerken_output = dict()
        for i in kenmerken_names:
            empty_dt = sec_kenmerken.find("dt", string=i)

            # Check if the <dt> is found and then find the corresponding <dd>
            if empty_dt:
                dd_element = empty_dt.find_next_sibling('dd')
                if dd_element:
                    # print("Found <dd> text:", dd_element.text.strip())
                    kenmerken_output[i] = dd_element.text.strip()
                    # return dd_element.text.strip()
                else:
                    print("No <dd> found after <dt> with empty text.")
            else:
                print("No <dt> with empty text found.")
        print(f"Num Kenmerken: {len(list(kenmerken_output.values()))}")
        return kenmerken_output
    
    def retrieve_kenmerken(self, soup):
        sec_kenmerken = soup.find('section', {'id': 'features'})
        if sec_kenmerken is None:
            print("No 'kenmerken' section found.")
            return {}  # Return an empty dictionary
        alle_kenmerken = self.kenmerken_extract(sec_kenmerken)
        wonen = re.search(r'\d+', alle_kenmerken['Wonen']).group() ## specify wonen/living_area
        
        kamers = re.findall(r'\d+', alle_kenmerken['Aantal kamers']) ## specify num kamers
        aantalkamers = max(map(int, kamers)) if kamers else None
        slaapkamers = min(map(int, kamers)) if kamers else None

        status = alle_kenmerken['Status']


        return wonen, alle_kenmerken, status
    
    def retrieve_phone(self, soup):
         ### Extract phone number for listing
        elements = soup.find_all(lambda tag: tag.name and tag.string and "Bel" in tag.string and any(char.isdigit() for char in tag.string)) ## look for word bel and numeric
        try:
            raw_text = elements[0].get_text()  # Get the text content from the element
            # Extract the phone number part using regex
            match = re.search(r"(\+?\(?\d+\)?[\d\- ]+)", raw_text)  # Match a phone number
            if match:
                phone_number = match.group(0)  # Extract the phone number from the match
                formatted_phone = self.format_number(phone_number)  # Format the number
                print("Formatted number:", formatted_phone)
                return formatted_phone
            else:
                print("No valid phone number found in:", raw_text)
                return raw_text
        except Exception as e:
            print(f'Phone number not found. {e}')
            formatted_phone = None
            return None
        
    @staticmethod
    def retrieve_makelaar_details(soup):
        makelaar_details = {'makelaar_url': None, 'makelaar_name': None}
        try:
            for a_tag in soup.find_all('a', href=True):  # Find <a> tags with href attribute
                href = a_tag['href']
                if 'https://www.funda.nl/makelaars/' in href.lower():  # Check if 'makelaar' is in href
                    # Get the title attribute (if exists), default to empty string if not
                    title = a_tag.get('title', '')
                    makelaar_details = {'makelaar_url':href, 'makelaar_name':title}
                    # return makelaar_details
                    break  # Stop the loop once you find the first match
        except Exception as e:
            print(f'Makelaar name not found. {e}')
            # makelaar_details = {'makelaar_url':None, 'makelaar_name':None}

        return makelaar_details
    
    def saving_output(self):
        updated_df = pd.concat([self.saved_df, self.new_listings_df], ignore_index=True)

        # Save the combined DataFrame back to output.parquet
        updated_df.to_parquet(self.output_file, index=False)
        updated_df = updated_df.drop_duplicates(subset=['listing_url'], keep='last')
        print("Updated data saved to output.parquet")
    def run(self):
        try:
            self.get_unprocessed_listings()
            ticks = 0
            listings = []
            for url in self.processing_listing_urls:
                ticks += 1
                print(f"Processed {ticks} of {len(self.processing_listing_urls)} new listings.")
                self.human_sleep()
                listings.append(self.scrape_listing(url))

            self.new_listings_df = pd.DataFrame(listings) 
        except Exception as e:
            print(f"Error during scraping: {e}")
        finally:
            self.driver.quit()
            if not self.new_listings_df.empty:
                self.saving_output()
            else:
                print("No new listings to save.")
            print('Finished processing.')