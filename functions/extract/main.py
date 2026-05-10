import requests
import json
import functions_framework
from datetime import datetime, timezone
from google.cloud import storage

GCS_BUCKET = "bacen-pipeline-data-lake"

INDICADORES = {
    "11":  {"nome": "Selic", "unidade": "% a.a."},
    "1":   {"nome": "Dolar PTAX", "unidade": "R$"},
    "433": {"nome": "IPCA", "unidade": "% a.m."},
    "12":  {"nome": "CDI", "unidade": "% a.a."},
    "189": {"nome": "IGP-M", "unidade": "% a.m."},
    "7326":{"nome": "IPCA Acumulado 12m", "unidade": "%"},
}

def get_serie(codigo, ultimos=10):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/{ultimos}?formato=json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def save_to_gcs(data, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json"
    )
    print(f"Salvo no GCS: {filename}")

@functions_framework.http
def extract_bacen(request):
    try:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%S")

        print("Iniciando extracao do Banco Central...")
        all_data = []

        for codigo, info in INDICADORES.items():
            print(f"Buscando {info['nome']} (codigo {codigo})...")
            try:
                serie = get_serie(codigo)
                for item in serie:
                    all_data.append({
                        "codigo": codigo,
                        "indicador": info["nome"],
                        "unidade": info["unidade"],
                        "data": item["data"],
                        "valor": float(item["valor"].replace(",", "."))
                    })
                print(f"  {len(serie)} registros encontrados")
            except Exception as e:
                print(f"  Erro em {info['nome']}: {e}")

        payload = {
            "extraction_date": today,
            "extraction_timestamp": timestamp,
            "total_registros": len(all_data),
            "indicadores": all_data
        }

        filename = f"bronze/bacen/{today}/indicadores_{timestamp.replace(':', '-')}.json"
        save_to_gcs(payload, filename)

        print(f"Extracao concluida! {len(all_data)} registros extraidos.")
        return {"status": "success", "registros": len(all_data), "file": filename}, 200

    except Exception as e:
        print(f"Erro: {e}")
        return {"status": "error", "message": str(e)}, 500
