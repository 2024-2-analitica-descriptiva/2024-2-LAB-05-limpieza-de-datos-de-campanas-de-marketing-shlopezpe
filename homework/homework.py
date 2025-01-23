"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortgage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cambiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    input_path = ('files/input/')
    output_path = ('files/output/')
    dataframe = read_zip_and_concat(input_path)
    
    save_csv_in_directory(client_data(dataframe), output_path, "client.csv")
    save_csv_in_directory(campaign_data(dataframe), output_path, "campaign.csv")
    save_csv_in_directory(economics_data(dataframe), output_path, "economics.csv")

    print("La operación fue un éxito.")

def save_csv_in_directory(dataframe, output_path, filename):
    """
    Guarda un DataFrame en un archivo CSV en el directorio especificado.
    """
    if not os.path.exists(output_path): 
        os.makedirs(output_path)
    output_file = os.path.join(output_path, filename)
    try: 
        dataframe.to_csv(output_file, index=False)
        print(f"Archivo guardado: {output_file}")
    except Exception as e: 
        print(f"Error al guardar el archivo: {e}")

def client_data(dataframe): 
    df_client = dataframe[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    df_client['job'] = df_client['job'].str.replace(r"[.]", "", regex=True).str.replace(r"[-]", "_", regex=True)
    df_client['education'] = df_client['education'].str.replace(r"[.]", "_", regex=True).replace("unknown", pd.NA)
    df_client['credit_default'] = df_client['credit_default'].apply(lambda x: 1 if x == "yes" else 0)
    df_client['mortgage'] = df_client['mortgage'].apply(lambda x: 1 if x == "yes" else 0)
    return df_client

    
def campaign_data(dataframe):
    df_campaign = dataframe[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'day', 'month']].copy()
    df_campaign['previous_outcome'] = df_campaign['previous_outcome'].apply(lambda x: 1 if x == "success" else 0)
    df_campaign['campaign_outcome'] = df_campaign['campaign_outcome'].apply(lambda x: 1 if x == "yes" else 0)
    df_campaign['last_contact_date'] = pd.to_datetime(df_campaign['day'].astype(str) + '-' + df_campaign['month'] + '-2022', format='%d-%b-%Y').dt.strftime('%Y-%m-%d') 
    df_campaign = df_campaign.drop(columns=['day', 'month'])
    return df_campaign

def economics_data(dataframe): 
    df_economics = dataframe[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()

def read_zip_and_concat(input_path): 
    if not os.path.isdir(input_path):
        raise ValueError(f"El directorio {input_path} no existe.")

    all_data = []
    for file_name in os.listdir(input_path):
        if file_name.endswith('.zip'):
            zip_path = os.path.join(input_path, file_name)
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                csv_files = [name for name in zip_file.namelist() if name.endswith('.csv')]
                if not csv_files:
                    print(f"No se encontraron archivos CSV en {file_name}")
                    continue
                for csv_file in csv_files:
                    with zip_file.open(csv_file) as file:
                        df = pd.read_csv(file)
                        all_data.append(df)
                        
    return pd.concat(all_data, ignore_index=True)


if __name__ == "__main__":
    clean_campaign_data()
