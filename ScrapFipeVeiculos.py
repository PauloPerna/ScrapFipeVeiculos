import requests
import re
from datetime import datetime
import mysql.connector as mc
import pandas as pd
import multiprocessing as mp
import sys

def GetBrands(data_table, vehicle_type, headers):
    url = 'https://veiculos.fipe.org.br/api/veiculos/ConsultarMarcas'
    payload = {
        'codigoTabelaReferencia': data_table,
        'codigoTipoVeiculo': vehicle_type
    }
    response = requests.post(url, data=payload, headers=headers)
    return response.json()

def ConsultModels(data_table, vehicle_type, brand_code, headers):
    url = 'https://veiculos.fipe.org.br/api/veiculos/ConsultarModelos'
    payload = {
        'codigoTipoVeiculo': vehicle_type,
        'codigoTabelaReferencia': data_table,
        'codigoMarca': brand_code,
    }
    response = requests.post(url, data=payload, headers=headers)
    return response.json()

def ConsultYearModel(data_table, vehicle_type, brand_code, model_code, headers):
    url = 'https://veiculos.fipe.org.br/api/veiculos/ConsultarAnoModelo'
    payload = {
        'codigoTipoVeiculo': vehicle_type,
        'codigoTabelaReferencia': data_table,
        'codigoModelo': model_code,
        'codigoMarca': brand_code
    }
    response = requests.post(url, data=payload, headers=headers)
    return response.json()

def QueryData(data_table, vehicle_type, brand_code, model_code, year_model_code, headers):
    url = 'https://veiculos.fipe.org.br/api/veiculos/ConsultarValorComTodosParametros'
    payload = {
        'codigoTabelaReferencia': data_table,
        'codigoMarca': brand_code,
        'codigoModelo': model_code,
        'codigoTipoVeiculo': vehicle_type,
        'anoModelo': year_model_code.split('-')[0],
        'codigoTipoCombustivel': year_model_code.split('-')[1],
        'tipoConsulta': 'tradicional',
    }
    response = requests.post(url, data=payload, headers=headers)
    return response.json()

def GetChunk(data_table, brand, vehicle_type, headers):
    """
    Retrieves data for all models and years of a given vehicle type and brand from a certain reference table.

    Args:
        data_table (str): The reference table code.
        brand (str): The brand name to retrieve data for.
        vehicle_type (int): The vehicle type code (1 for cars, 2 for motorcycles, and 3 for trucks/tractors).
        headers (dict): The headers to be used in the HTTP request.

    Returns:
        tuple: A tuple consisting of a boolean indicating success or failure of the function, and a list containing the retrieved data for all models and years of the specified brand and vehicle type from the given reference table.
    """
    try:
        count = 0
        chunk_data = []
        models = ConsultModels(data_table, vehicle_type=vehicle_type, brand_code=brand, headers=headers)
        if 'Modelos' not in models or not isinstance(models['Modelos'], list):
            return False, chunk_data
        print('This brand has', len(models['Modelos']), 'models')
        for mo in range(0, len(models['Modelos'])):
            model = models['Modelos'][mo]['Value']
            year_models = ConsultYearModel(data_table, vehicle_type=vehicle_type, brand_code=brand, model_code=model, headers=headers)
            if not isinstance(year_models, list):
                continue
            for year in range(0, len(year_models)):
                year_model = year_models[year]['Value']
                data = QueryData(data_table, vehicle_type=vehicle_type, brand_code=brand, model_code=model, year_model_code=year_model, headers=headers)
                data.update({'data_tabela': data_table, 'brand_code': brand, 'model_code': model, 'year_model_code': year_model})
                chunk_data.append(data)
                count += 1
        return True, chunk_data
    except:
        return False, []

def CleanData(chunk_data):
    """
    This function receives a chunk of data scraped from a website and converts it into a pandas DataFrame. 
    Then, it cleans the 'Valor' column, removing any characters that are not digits or a comma, and converts 
    the comma to a period before casting the column to a float.
    """
    clean_data = pd.DataFrame(chunk_data)
    clean_data['Valor'] = clean_data['Valor'].apply(lambda x: float(re.sub('[^0-9,]', '', x).replace(',', '.')))
    return clean_data

def UploadChunkData(chunk_data, connection, cursor):
    cleaned_data = CleanData(chunk_data)
    data_tuples = [tuple(row) for row in cleaned_data.values]
    insert_query = ('''INSERT INTO scraped_data 
        (value, brand, model, year_model, fuel, fipe_code, reference_month, 
        authentication, vehicle_type, fuel_initials, query_date, reference_month_code, 
        brand_code, model_code, year_model_code) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''')
    cursor.executemany(insert_query, data_tuples)
    connection.commit()
    return True

def GetAllData(data_table, vehicle_type, user, password):
    # This piece of code is very specific to my use case, where I'm uploading data to my own database.
    # You may find it useful to rewrite this function with your own logic.
    connection = mc.connect(user=user, password=password, host='database_host', database='database_name')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM brands_table WHERE reference_month = %s AND vehicle_type = %s", (data_table, vehicle_type))
    brands = cursor.fetchall()
    brands = [b[1] for b in brands]
    # Filter out the brands that have already been scraped
    cursor.execute('''SELECT DISTINCT brand_val FROM scraped_data 
            WHERE reference_month = %s
            AND vehicle_type = %s
            ''', (data_table, vehicle_type))
    scraped_brands = cursor.fetchall()
    scraped_brands = [b[0] for b in scraped_brands]
    brands = [b for b in brands if b not in scraped_brands]
    headers = {
        'Referer': 'https://veiculos.fipe.org.br/',
        'Content-Type': 'application/json'
    }
    for brand in brands:
        success = False
        while not success:
            print("trying to get chunk...", datetime.now())
            success, chunk_data = GetChunk(data_table, brand, vehicle_type, headers)
        UploadChunkData(chunk_data, connection, cursor)
    return True

if __name__ == '__main__':
    # This piece of code is very specific to my use case, where I'm uploading data to my own database.
    # You may find it useful to rewrite this function with your own logic.
    num_workers = int(sys.argv[1])
    vehicle_type = sys.argv[2]
    user = sys.argv[3]
    password = sys.argv[4]
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }
    # Note that I have already mapped all date codes and brands to be scraped. 
    # This is useful to speed up the scraping process and to track the data scraping progress.
    connection = mc.connect(user=user, password=password, host='database_host', database='database_name')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM data_table WHERE to_scrape = 1")
    all_data_table = cursor.fetchall()
    all_data_table = [data[1] for data in all_data_table]
    pool = mp.Pool(num_workers)
    results = pool.starmap(GetAllData, [(data, vehicle_type, user, password) for data in all_data_table])
