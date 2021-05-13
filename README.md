# Capstone 2021

The 2021 Capstone Group from CU Boulder built this crawler to download blob objects, extract metadata from them and then publish that data to an analytics platform. For this project the capstone students targeted Trimble's own storage and analytics services.

---

# trimble-cloud-crawler
  
Crawler API   
/crawler – Returns a json string with general infromation about the crawler object.  
/crawler/setup?path=\<path\> – setup the crawler fresh to start crawling at the directory from path variable.  
/crawlers/start – starts the crawl.  
/search - Used to visulize the indexes the crawler has created.  
   
Running Flask API on Windows  
$env:FLASK_APP = "flask_crawler.py"  
python -m flask run
  
Notes  
Cralwer is limiting how many pages and files it will extract metadata from.  


  
