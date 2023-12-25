### 1. Create virtual environment 
    $ python3 -m venv venv
### 2. Install packages
    $ pip install -r requirements.txt
### 3. Create .env file
### 4. Make run_spider.sh file executable
    $ chmod +x run_spider.sh
### 5. Configure chrontab job
    $ crobtab -e 
### 6. Add cronjob
    0 19 * * * /home/ganaa/projects/unegui_properties/run_spider.sh
