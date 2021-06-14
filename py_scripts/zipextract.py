import os,sys

def write_to_table(database_name):
  os.system(f"{sys.executable} -m pip install --upgrade pytd")
  import pytd

  #Sample files
  complete_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
  temp_file = '/tmp/movie.zip'
  files = ['ratings.csv', 'movies.csv', 'tags.csv', 'links.csv']
  print(complete_dataset_url)
  
  #
  # Download Movielens dataset
  #
  import urllib.request
  urllib.request.urlretrieve(complete_dataset_url, temp_file)
  print('Downloaded zip file')
  
  #
  # Extract Zipped file
  #
  import zipfile
  with zipfile.ZipFile(temp_file, 'r') as z:
    zipInfo=z.infolist()
    for isiZip in zipInfo :
      z.extract(isiZip,path='/tmp/dataset/')
  
  print('Extracted zip file')

  import pandas as pd
  #
  # Import data to Treasure CDP
  #
  apikey = os.environ['TD_API_KEY']
  apiserver = os.environ['TD_API_SERVER']

  client = pytd.Client(apikey=apikey, endpoint=apiserver, database=database_name)
  
  client.create_database_if_not_exists(database_name)
  
  for item in files:
    tbl_name = item.split('.')[0]
    df = pd.read_csv(f"/tmp/dataset/ml-latest/{item}")
    print(f"Importing {tbl_name}")
    client.load_table_from_dataframe(df, f"{database_name}.{tbl_name}", writer='bulk_import', if_exists="overwrite")
#    print(f"{database_name}.{tbl_name}")

    print(f"Imported {tbl_name}")

  print('All Done')

if __name__ == "__main__":
    write_moviedataset_to_table("sample_database")
