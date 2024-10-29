import dlt
from dlt.sources.rest_api import rest_api_source

API_KEY = "5be0226f960d762c0448f7c55d9eb986"
#API_KEY = "d0b5c0b5572003d628dff99591e45d4d"
CITY = 'London'

api_url = 'http://api.openweathermap.org/data/2.5/weather'
params = {
    'q': CITY,
    'appid': API_KEY
}

# Define the API endpoint and parameters
source = rest_api_source({
     "client":  {
         "base_url": "http://api.openweathermap.org/",
         "auth": {
             "token": API_KEY
         }
     },
     "resources": [
        {
            "name": "weather_data",
            "endpoint":
               {
                  "path": "data/2.5/weather",
                  "params": params
            }
        }
     ]
})

#  https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API
# https://api.openweathermap.org
# Create a dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name='weather_data_pipeline',
    destination='duckdb',  # You can change this to your desired destination
    dataset_name='weather_data'
)

# pipeline = dlt.pipeline(
#     pipeline_name="pickle_weather",
#     destination="json",
#     dataset_name="pickle_data",
# )

# Main function to run the pipeline
def main():
    # # Run the pipeline with the weather data source
    # info = pipeline.run(source)
    # print(info)

    # Fetch data using dlt
       data = next(source)  # Assuming the source yields data

       # Write data to a JSON file
       with open('weather_data.json', 'w') as json_file:
           json.dump(data, json_file, indent=4)

       print("Data written to weather_data.json")

if __name__ == '__main__':
    main()