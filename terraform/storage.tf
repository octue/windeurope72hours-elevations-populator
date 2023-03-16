resource "google_storage_bucket" "crash_diagnostics" {
  name                        = "octue-elevations-populator-private"
  location                    = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}
