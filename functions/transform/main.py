import json
import functions_framework
from datetime import datetime, timezone
from google.cloud import storage, bigquery

GCS_BUCKET = "bacen-pipeline-data-lake"
BQ_PROJECT = "bacen-pipeline"
BQ_DATASET = "analytics"
BQ_TABLE = "indicadores_economicos"

def read_from_gcs(filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    return json.loads(blob.download_as_string())

def load_to_bigquery(rows):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"Erros ao inserir no BigQuery: {errors}")
    print(f"Inseridos {len(rows)} registros no BigQuery")

@functions_framework.http
def transform_bacen(request):
    try:
        request_json = request.get_json(silent=True)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = request_json.get("filename") if request_json else None

        if not filename:
            filename = f"bronze/bacen/{today}/test.json"

        print(f"Lendo arquivo: {filename}")
        raw_data = read_from_gcs(filename)

        indicadores = raw_data.get("indicadores", [])
        extraction_date = raw_data.get("extraction_date", today)
        extraction_timestamp = raw_data.get("extraction_timestamp", "")

        rows = []
        for item in indicadores:
            rows.append({
                "indicador": item["indicador"],
                "codigo": item["codigo"],
                "data": item["data"],
                "valor": float(item["valor"]),
                "unidade": item["unidade"],
                "extraction_date": extraction_date,
                "extraction_timestamp": extraction_timestamp
            })

        load_to_bigquery(rows)
        return {"status": "success", "rows_inserted": len(rows)}, 200

    except Exception as e:
        print(f"Erro: {e}")
        return {"status": "error", "message": str(e)}, 500
