-- requires udf_get_key()
CREATE TEMP FUNCTION
  udf_get_key(map ANY TYPE,
    k ANY TYPE) AS ( (
    SELECT
      key_value.value
    FROM
      UNNEST(map.key_value) AS key_value
    WHERE
      key_value.key = k
    LIMIT
      1 ) );
-- defines udf_aggregate_search_counts()
CREATE TEMP FUNCTION
  udf_aggregate_search_counts(search_counts ARRAY<STRUCT<list ARRAY<STRUCT<element STRUCT<engine STRING,
    source STRING,
    count INT64>>>>>) AS ((
    SELECT
      AS STRUCT --
      abouthome + contextmenu + newtab + searchbar + system + urlbar AS `all`,
      *
    FROM (
      SELECT
        AS STRUCT COALESCE(udf_get_key(search_counts,
            "abouthome"),
          0) AS abouthome,
        COALESCE(udf_get_key(search_counts,
            "contextmenu"),
          0) AS contextmenu,
        COALESCE(udf_get_key(search_counts,
            "newtab"),
          0) AS newtab,
        COALESCE(udf_get_key(search_counts,
            "searchbar"),
          0) AS searchbar,
        COALESCE(udf_get_key(search_counts,
            "system"),
          0) AS system,
        COALESCE(udf_get_key(search_counts,
            "urlbar"),
          0) AS urlbar
      FROM (
        SELECT
          ARRAY(
          SELECT
            AS STRUCT element.source AS key,
            SUM(element.count) AS value
          FROM
            UNNEST(search_counts),
            UNNEST(list)
          GROUP BY
            element.source) AS key_value) AS search_counts )));
