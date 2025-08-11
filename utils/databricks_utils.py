import requests

def download_query(cookie, x_csrf_token, query):
    response = (
        requests
        .post(
            "https://dbc-9ea5c36b-a253.cloud.databricks.com/notebook/2259370766326036/command/7855768847358246",
            headers = {
                "Content-Type": "application/json",
                "Cookie": cookie,
                "x-csrf-token": x_csrf_token
            },
            json = {
                "changes": {
                    "command": query,
                    "commandVersion": 1,
                    "lastModifiedBy": "502c0e9b-3047-450a-b07c-e86743e15d93"
                },
                "@method": "updateCmd"
            }
        )
    )

    response = requests.post(
        "https://dbc-9ea5c36b-a253.cloud.databricks.com/notebook/2259370766326036/command/7855768847358246",
        headers = {
            "Content-Type": "application/json",
            "Cookie": cookie,
            "x-csrf-token": x_csrf_token
        },
        json = {
            "newBindings": {},
            "saveResultsToDBFS": True,
            "executeWithV2ResultLimits": False,
            "@method": "runExistingCmd"
        }
    )

    response = requests.post(
        "https://dbc-9ea5c36b-a253.cloud.databricks.com/full-command-results/7855768847358246?csrf=7d7b8f66-d0d0-43fe-a84c-b4bf9e771192&o=183385582004660&downloadFormat=CSV",
        headers = {
            "Content-Type": "application/json",
            "Cookie": cookie,
            "x-csrf-token": x_csrf_token
        },
        json = {
            "csrf": "7d7b8f66-d0d0-43fe-a84c-b4bf9e771192",
            "o": "183385582004660",
            "downloadFormat": "CSV"
        }
    )

    return response.content

with open("resultado.csv", "wb") as f:
    f.write(download_query(cookie, x_csrf_token, "select 2 as b, 3 as c"))




























############### 10/08/2025



@pytest.fixture
def setup_data(request, volume_acess_strategy: VolumeAccessStrategy):
    test_name = request.function.__name__  # nome da função de teste, ex: 'test_abc'
    test_file_path = str(request.fspath)  # ex: '/path/to/tests/test_my_etl.py'

    # Monta o caminho para data/<nome_do_teste>
    # Considerando que 'data' está no mesmo diretório dos testes
    test_dir = os.path.dirname(test_file_path)  # diretório onde está o arquivo do teste
    data_dir = os.path.join(test_dir, "data", test_name)

    print("rodando: ", test_name)
    print("rodando: ", data_dir)





import os
import json
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

convert_operations: Dict[str, Any] = {
    "date": (lambda v: datetime.strptime(v, "%Y-%m-%d").date()),
    "decimal": (lambda v: Decimal(v)),
    "timestamp": (lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S")),
}

def convert_data_types(data: List[Dict[str, Any]], schema: StructType) -> List[Dict[str, Any]]:    
    converted_data: List[Dict[str, Any]] = []
    for row in data:
        converted_row: Dict[str, Any] = {}

        for schema_column in schema:
            if (schema_column.dataType.typeName() in convert_operations.keys()):
                converted_row[schema_column.name] = convert_operations[schema_column.dataType.typeName()](row[schema_column.name])
            else:
                converted_row[schema_column.name] = row[schema_column.name]

        converted_data.append(converted_row)

    return converted_data

def read_all_json_files(spark_session: SparkSession, directory: str) -> None:
    json_files_paths: List[str] = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.json')]
    for file_path in json_files_paths:
        json_file: Dict[str, Any] = json.load(open(file_path, encoding='utf-8'))
        schema: StructType = StructType.fromJson(json_file["schema"])
        data: List[Dict[str, Any]] = convert_data_types(json_file["data"], schema)
        
        data_frame: DataFrame = (
            spark_session
            .createDataFrame(
                data = data,
                schema = schema
            )
        )

        display(data_frame)

read_all_json_files(spark, "/Workspace/Users/danielbaquini@gmail.com/databricks-pipeline-kit/src/")




# catalog.schema.table.json
{
    "schema": {
        "version": "1.0",
        "type": "struct",
        "fields": [
            { "name": "id", "type": "integer", "nullable": false },
            { "name": "price", "type": "decimal(17, 2)" },
            { "name": "quantity", "type": "integer" },
            { "name": "discount_rate", "type": "float" },
            { "name": "is_active", "type": "boolean" },
            { "name": "order_date", "type": "date" },
            { "name": "last_update", "type": "timestamp" }
        ],
        "table": {
            "data_path": [],
            "data_name": "",
            "data_layer": ""
        }
    },
    "data": [
        {
            "id": 101,
            "price": "1234.56",
            "quantity": 3,
            "discount_rate": 0.15,
            "is_active": true,
            "order_date": "2025-08-10",
            "last_update": "2025-08-10 18:42:30"
        },
        {
            "id": 102,
            "price": "499.99",
            "quantity": 10,
            "discount_rate": 0.05,
            "is_active": false,
            "order_date": "2025-08-09",
            "last_update": "2025-08-09 09:15:00"
        },
        {
            "id": 103,
            "price": "25000.00",
            "quantity": 1,
            "discount_rate": 0.20,
            "is_active": true,
            "order_date": "2025-08-08",
            "last_update": "2025-08-08 14:00:00"
        }
    ]

}




# conftest.py
@pytest.fixture
def setup_data(request, volume_acess_strategy: VolumeAccessStrategy):
    test_name = request.function.__name__  # nome da função de teste, ex: 'test_abc'
    test_file_path = str(request.fspath)  # ex: '/path/to/tests/test_my_etl.py'

    # Monta o caminho para data/<nome_do_teste>
    # Considerando que 'data' está no mesmo diretório dos testes
    test_dir = os.path.dirname(test_file_path)  # diretório onde está o arquivo do teste
    data_dir = os.path.join(test_dir, "data", test_name)

    print("rodando: ", test_name)
    print("rodando: ", data_dir)


@pytest.mark.usefixtures("clear_global_volume_before_test", "setup_data")
def test_etl_with_global_volume_3(volume_strategy_global):
    csv1 = "/Volumes/workspace/default/base_volume/input1.csv"
    csv2 = "/Volumes/workspace/default/base_volume/input2.csv"

    df, table, elapsed = measure_time_and_run(
        volume_strategy_global.spark,
        csv1,
        csv2,
        volume_strategy_global.volume_id,
        suffix="_3"
    )
    assert df.count() > 0
