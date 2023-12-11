# TCSS462TermProject

clone repo into directory

# Create python virtual environment

In base repo directory run,

source venv/bin/activate

# Install dependencies

run in virtual environment,

pip install -r requirements.txt


# Query Request Example
```
{
  "bucket_name" : "service1csvbucket",
  "key" : "file_name",
  "Filters": {
    "Region": "Sub-Saharan Africa",
    "Item Type": "Snacks",
    "Sales Channel": "Offline",
    "Order Priority": "Low",
    "Country": "Zambia"
  },
  "Group By": [
    "Region",
    "ItemType"
  ]
}

```
