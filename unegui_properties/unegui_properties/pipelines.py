# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from collections import defaultdict
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
# from models import Properties, PropertyImage, db_settings
from models.properties import Properties, PropertyImage
from models import db_settings
from models.base import session_scope, Base
import re

def remove_second_occurrences_and_comma(text):
    # Function to replace the second occurrences
    def replace_second(match):
        word = match.group(0)
        occurrences[word] += 1
        if occurrences[word] == 2:
            # Remove the word and following comma and space if they exist
            return ''
        return word

    # Dictionary to count occurrences
    occurrences = defaultdict(int)

    # Regex to find all words/phrases (assuming words are separated by spaces or commas)
    # This pattern also captures a following comma and space if they exist
    modified_text = re.sub(r'[\wа-яА-Я]+(?:, )?', replace_second, text)
    
    return modified_text

class UneguiPropertiesPipeline:
    def process_item(self, item, spider):
        return item



class CleanPipeline:
    def process_item(self, item, spider):
        # Clean 'Тагт:'
        if 'Тагт:' in item['property_details']:
            item['property_details']['Тагт:'] = self.extract_number(item['property_details']['Тагт:']) or 0
        
        if 'Талбай:' in item['property_details']:
            item['property_details']['Талбай:'] = float(item['property_details']['Талбай:'].split()[0])
        # Add more cleaning steps as needed
        
        if 'Ашиглалтанд орсон он:' in item['property_details']:
            item['property_details']['Ашиглалтанд орсон он:'] = self.extract_number(item['property_details']['Ашиглалтанд орсон он:'])
            
        if 'Барилгын давхар:' in item['property_details']:
            item['property_details']['Барилгын давхар:'] = self.extract_number(item['property_details']['Барилгын давхар:'])
        
        if 'Хэдэн давхарт:' in item['property_details']:
            item['property_details']['Хэдэн давхарт:'] = self.extract_number(item['property_details']['Хэдэн давхарт:'])

        if 'Цонхны тоо:' in item['property_details']:
            item['property_details']['Цонхны тоо:'] = self.extract_number(item['property_details']['Цонхны тоо:'])

        
        # if item['loan_info']:
        #     try:
        #         item['loan_info']['monthly_payment'] = int(''.join(item['loan_info']['monthly_payment'].split()[0].split(',')))
        #     except:
        #         item['loan_info']['monthly_payment'] = float(item['loan_info']['monthly_payment'].split()[0]) * (10**6)
                
        #     item['loan_info']['term'] = self.extract_number(item['loan_info']['term'])
            
        if item['brand']:
            if 'зарна' in item['brand']:
                item['sell_type'] = 'sell'
            if 'түрээс' in item['brand']:
                item['sell_type'] = 'rent'

            item['brand'] = item['brand'].replace(' зарна', '')

        if item['model']:
            item['model'] = item['model'].replace(' зарна', '')

        if item['price']:
            price_txt = item['price'].lower()
            item['price_txt'] = price_txt
            if item['brand'] in  ['Орон сууц', 'Үл хөдлөх']:
                if 'сая' in price_txt:
                    number = self.extract_number(price_txt)
                    if number < 20 and item['model'] != 'Гараж, контейнер, з-сууц' and 'Талбай:' in item['property_details']:
                        item['price'] = item['property_details']['Талбай:'] * number * (10**6)
                    else:
                        item['price'] = self.extract_number(price_txt) * (10**6)
                elif 'тэрбум' in price_txt:
                    item['price'] = float(price_txt.split()[0]) * (10**9)

        if item['model']:
            if 'өрөө' in item['model']:
                item['room_number'] = int(self.extract_number(item['model']))
            else:
                item['room_number'] = None
                
        if item['location']:
            item['location'] = remove_second_occurrences_and_comma(item['location'])
            location =  item['location']
            province, district, khoroo = '', '', ''
            if 'УБ' in location:
                # province, district =  location.split(',')[0].split('—', 1)
                splitted = location.split('—')
                province = splitted[0]
                district = splitted[1].split(',')[0]
                if len(splitted[1].split(',')) > 1:
                    khoroo = splitted[1].split(',')[1]
            else:
                province, district = location.split(',')
                
            item['province'] = province.strip()
            item['district'] = district.strip()
            item['khoroo'] = khoroo.replace('-', '').strip()

        return item

    def extract_number(self, text):
        match = re.search(r'\d+\.\d+|\d+', text)
        return float(match.group()) if match else None


class PostgresPipeline(object):
    def __init__(self):
        """
        Initializes database connection and creates tables.
        """
        # Initialize the database and create tables if they don't exist
        engine = create_engine(URL.create(**db_settings.DATABASE))
        Base.metadata.create_all(engine)

    def process_item(self, item, spider):
        with session_scope() as session:
            try:
                latitude = item['latitude']
                longitude = item['longitude']

                if latitude and longitude:
                    location = 'SRID=4326;POINT({} {})'.format(longitude, latitude)
                else:
                    location = None

                existing_property = session.query(Properties).filter_by(link=item['link']).first()

                if existing_property:
                    existing_property.lat = latitude
                    existing_property.long = longitude
                    existing_property.location = location
                else:
                    details = item['property_details']
                    property_type = 'Орон сууц' if item['brand'] == 'Орон сууц' else item.get('model', None)

                    property = Properties(
                        link = item['link'],
                        rooms = item.get('room_number', 0),
                        garage = details.get('Гараж:',None),
                        balcony_number = details.get('Тагт:', None),
                        area = details.get('Талбай:', None),
                        door = details.get('Хаалга:', None),
                        window = details.get('Цонх:', None),
                        floor = details.get('Шал:', None),
                        window_number = details.get('Цонхны тоо:', None),
                        building_floor = details.get('Барилгын давхар:', None),
                        which_floor = details.get('Хэдэн давхарт:', None),
                        commission_year = details.get('Ашиглалтанд орсон он:', None),
                        leasing = details.get('Лизингээр авах боломж:', None),
                        progress = details.get('Барилгын явц:', None),
                        location = location,
                        price = item.get('price', None),
                        province = item.get('province', None),
                        district = item.get('district', None),
                        khoroo = item.get('khoroo', None),
                        sell_type = item.get('sell_type', None),
                        property_type = property_type,
                        lat=latitude,
                        long=longitude
                    )

                    session.add(property)
                    session.flush()  # Flush to get the generated ID for car_listing

                    for img_url in item['img_urls']:
                        property_image = PropertyImage(property_id=property.id, img_url=img_url)
                        session.add(property_image)

     

                # session.commit() is automatically called by the session_scope context manager
            except Exception as e:
                spider.logger.error(f"Error saving item to database: {e}")
                # session.rollback() is automatically called by the session_scope context manager
                raise
        return item
