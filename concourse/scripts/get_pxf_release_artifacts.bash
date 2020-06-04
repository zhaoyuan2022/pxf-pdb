#!/usr/bin/env bash

set -e

: "${GCS_RELEASES_BUCKET:?GCS_RELEASES_BUCKET must be set}"
: "${GCS_RELEASES_PATH:?GCS_RELEASES_PATH must be set}"
: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"

copy_to_artifacts_dir() {
  local source=gs://${GCS_RELEASES_BUCKET}/${GCS_RELEASES_PATH}/gp${1}/${2}
  local destination=pxf_artifacts
  if gsutil ls "${source}" >/dev/null 2>&1; then
    echo "Copying ${source} to ${destination}..."
    gsutil cp "${source}" "${destination}"
  fi
}

echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

version=$(< pxf_open_source_license_file/version)

copy_to_artifacts_dir 5 "pxf-gp5-${version}-1.el6.x86_64.rpm"
copy_to_artifacts_dir 5 "pxf-gp5-${version}-1.el7.x86_64.rpm"
copy_to_artifacts_dir 6 "pxf-gp6-${version}-1.el7.x86_64.rpm"
