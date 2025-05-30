from typing import Iterable
import scrapy
from scrapy.http import Request

class UneguiSpider(scrapy.Spider):
    name = 'unegui'

    def start_requests(self):
        urls = ['https://www.unegui.mn/l-hdlh/l-hdlh-zarna/']

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    
    def parse(self, response):
        advert_contents = response.css('div.advert__content')
        for advert_content in advert_contents:
            href = advert_content.css('a::attr(href)').get()
            # print(href)
            yield scrapy.Request(url='https://unegui.mn'+href, callback=self.parse_details)

        next_page = response.css('a.number-list-next::attr(href)').get()
        # if next_page is not None:
        #     yield response.follow(next_page, self.parse)

    def parse_details(self, response):
        property_details = {}
        img_urls = []

        for char in response.css('.announcement-characteristics .chars-column li'):
            key = char.css('.key-chars::text').get().strip()
            value = char.css('.value-chars::text').get().strip()
            property_details[key] = value

        texts = response.css('.announcement-description .js-description').css('p::text').getall()
        description = '\n'.join(texts)

        images = response.css('.announcement__images-item')

        for img in images:
            image_url = img.css('::attr(src)').get()
            img_urls.append(image_url)

        price_div = response.css('.announcement-price__cost')
        price = price_div.xpath('normalize-space(string(.))').get()

        breadcrumb = response.css('ul.breadcrumbs li[itemprop="itemListElement"]')
        brand = breadcrumb[-2].css('span[itemprop="name"]::text').get()
        model = breadcrumb[-1].css('span[itemprop="name"]::text').get()
        tags = [breadcrumb[i].css('span[itemprop="name"]::text').get() for i in range(3, len(breadcrumb))]
        location = response.css('span[itemprop="address"]::text').get()

         # Extract the data-coords attribute value
        data_coords = response.css('.js-open-announcement-location::attr(data-coords)').get()

        lon, lat = None, None

        if data_coords:
            # Removing the unnecessary string parts and splitting by space
            lon, lat = data_coords.replace('SRID=4326;POINT (', '').replace(')', '').split()

            # Convert to float if needed
            lon = float(lon)
            lat = float(lat)
        
        yield {
            'link': response.url,
            'property_details': property_details,
            'description': description,
            'img_urls': img_urls,
            'price': price,
            'link': response.url,
            'brand': brand,
            'model': model,
            'tags': tags,
            'location': location,
            'data_coords': data_coords,
            'latitude': lat,
            'longitude': lon
        }