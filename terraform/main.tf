terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("bacen-pipeline-e7b5d19acd81.json")
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

resource "google_bigquery_table" "indicadores" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "indicadores_economicos"
  deletion_protection = false

  schema = jsonencode([
    { name = "indicador", type = "STRING" },
    { name = "codigo", type = "STRING" },
    { name = "data", type = "STRING" },
    { name = "valor", type = "FLOAT" },
    { name = "unidade", type = "STRING" },
    { name = "extraction_date", type = "STRING" },
    { name = "extraction_timestamp", type = "STRING" }
  ])
}
