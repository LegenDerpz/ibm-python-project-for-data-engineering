import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime as dt, date
from pathlib import Path

dir = Path(__file__).parent
log_file = f"{dir}/log_file.txt"
target_file = f"{dir}/transformed_data.csv"

def extract_from_csv(file):
    df = pd.read_csv(file)
    return df

def extract_from_json(file):
    df = pd.read_json(file, lines=True)
    return df

def extract_from_xml(file):
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()
    
    for car in root:
        model = car.find("car_model").text
        year_of_manufacture = date(int(car.find("year_of_manufacture").text), 1, 1)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{'car_model':model, 'year_of_manufacture':year_of_manufacture, 'price':price, 'fuel':fuel}])], ignore_index=True)
        
def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    for csv in glob.glob(f"{dir}/data/*.csv"):
        if csv != target_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csv))], ignore_index=True)
        
    for json in glob.glob(f"{dir}/data/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(json))], ignore_index=True)
        
    for xml in glob.glob(f"{dir}/data/*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xml))], ignore_index=True)
        
    return extracted_data

def transform(data):
    data["price"] = pd.to_numeric(data["price"], errors="coerce")
    data['price'] = round(data.price, 2)
    
    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)
    
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = dt.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f'{timestamp}, {message}\n')
        
log_progress("ETL Job Started")

log_progress("Extract Phase Started")
extracted_data = extract()

log_progress("Extract Phase Ended")

log_progress("Transform Phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

log_progress("Transform Phase Ended")

log_progress("Load Phase Started")
load_data(target_file, transformed_data)

log_progress("Load Phase Ended")

log_progress("ETL Job Ended")