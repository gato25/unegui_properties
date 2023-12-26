#!/bin/bash
cd /home/ganaa/projects/unegui_properties
source venv/bin/activate && cd unegui_properties  # Activate virtual environment
scrapy crawl unegui    # Replace 'my_spider' with your spider name
deactivate