def day_snapshot_sql(year: int, month: int, day: int, rhel_major: int = 10) -> str:
    return f"""
    WITH day_slice AS (
      SELECT
        org_id, ebs_account, inventory_id, request_id, uploaded_at,
        rhel_major, rhel_minor, virt_what_info,
        name, version, release, arch, epoch, vendor,
        created_year, created_month, created_day
      FROM s3_datahub_insights.insights_wh_extraction_rules.installed_rpms
      WHERE created_year = {year}
        AND created_month = {month}
        AND created_day = {day}
        AND rhel_major = {rhel_major}
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
     AND ds.uploaded_at  = lu.uploaded_at
    """

