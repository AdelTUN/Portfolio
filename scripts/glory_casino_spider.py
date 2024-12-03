import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

class GloryCasinoSpider(scrapy.Spider):
    name = 'glory_casino'
    start_urls = ['https://glory.casino/']

    def __init__(self, *args, **kwargs):
        super(GloryCasinoSpider, self).__init__(*args, **kwargs)
        self.visited_urls = set()
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run headless Chrome
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        self.driver.set_page_load_timeout(30)  # Set page load timeout
        self.depth_limit = 2  # Set the depth limit for crawling

    def parse(self, response):
        self.driver.get(response.url)

        # Wait for the content to load
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
            )
        except Exception as e:
            self.logger.error(f"Error while loading page {response.url}: {e}")
            return

        # Extract HTML content after JavaScript has rendered
        rendered_body = self.driver.page_source

        # Save the content of the page
        page = response.url.split("/")[-2] or "index"
        filename = f'{page}.html'
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(rendered_body)

        # Log visited URLs
        self.visited_urls.add(response.url)

        # Follow links to other pages if depth limit is not reached
        if response.meta.get('depth', 1) <= self.depth_limit:
            for next_page in self.driver.find_elements(By.CSS_SELECTOR, 'a'):
                next_page_url = next_page.get_attribute('href')
                if next_page_url and next_page_url not in self.visited_urls:
                    self.visited_urls.add(next_page_url)
                    yield scrapy.Request(next_page_url, callback=self.parse, meta={'depth': response.meta.get('depth', 1) + 1})

    def closed(self, reason):
        self.driver.quit()
        # Save visited URLs to a file when the spider closes
        with open('visited_urls.txt', 'w') as f:
            for url in sorted(self.visited_urls):
                f.write(f"{url}\n")
