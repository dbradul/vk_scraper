# VK Scraper

Fetches VK user according to given search criteria. 

### Cnfiguration:
Config.py contains configuration settings in a form of python dict 
- `search_criteria`: search criteria used for fetching. Only fields from the list are supported: https://vk.com/dev/users.search  
- `fetch_fields`: user fields used for fetching. Only fields from the list are supported: https://vk.com/dev/fields  
- `csv_fields`: absolute list of field in output csv file
- `search_count`: [optional] fetching page size


### To fetch users using search criteria from config.py file:
```
python3 main.py
```


### To fetch users using ids from provided csv file:
```
python3 main.py csv_file
```
- `csv_file`: input file with column `id`, which is used to fetch user info 


### To fetch users using column from provided csv file:
```
python3 main.py csv_file column_name
```
- `csv_file`: input file with column `<column_name>` 
- `column_name`: column name, which name is used as search field name and values as search field values 

Example:
```
python3 main.py ParsedTanksToFind.csv screen_name
```


### To dump cities and universities id->name mappings:
```
python3 main.py dump
```
- `dump`: command to dump mappings into `mappings.json` file. If mappings already exist they will be updated (upsert mode).
