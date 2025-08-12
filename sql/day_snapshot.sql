-- Pull a single day of RHEL 10 data, keeping ALL packages from each system's latest upload.
-- Notes:
-- - Keep vendor filter to Red Hat
-- - BaseOS/AppStream filtering is added later
-- - Kernel filtering happens in Python after load

WITH day_slice AS (
  SELECT
    org_id,
    ebs_account,
    inventory_id,
    request_id,
    uploaded_at,
    rhel_major,
    rhel_minor,
    virt_what_info,
    name,
    version,
    release,
    arch,
    epoch,
    vendor,
    created_year,
    created_month,
    created_day
  FROM s3_datahub_insights.insights_wh_extraction_rules.installed_rpms
  WHERE created_year = 2025
    AND created_month = 7
    AND created_day = 30
    AND rhel_major = 10
    AND LOWER(vendor) = 'red hat, inc.'
),
latest_upload AS (
  SELECT inventory_id, MAX(uploaded_at) AS uploaded_at
  FROM day_slice
  GROUP BY inventory_id
)
SELECT ds.*
FROM day_slice ds
JOIN latest_upload lu
  ON ds.inventory_id = lu.inventory_id
 AND ds.uploaded_at  = lu.uploaded_at;
