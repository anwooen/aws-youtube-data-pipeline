-- Athena queries used to validate, analyze, and benchmark
-- the YouTube AWS data engineering pipeline.

-- Raw S3 data -> cleaned Parquet tables -> final analytics table

-- Key benchmark:
-- Join query runtime improved from 8.547s to 1.416s
-- by precomputing joins into final_analytics.

-- 1. Preview cleaned video statistics data
SELECT *
FROM db_youtube_cleaned.raw_statistics
LIMIT 10;

-- 2. Preview cleaned category reference data
SELECT *
FROM db_youtube_cleaned.cleaned_statistics_reference_data
LIMIT 10;

-- 3. Validate join between video statistics and category data
SELECT
    a.title,
    a.category_id,
    b.snippet_title
FROM db_youtube_cleaned.raw_statistics a
INNER JOIN db_youtube_cleaned.cleaned_statistics_reference_data b
    ON a.category_id = b.id
LIMIT 20;

-- 4. Validate joined results for a specific region
SELECT
    a.title,
    a.category_id,
    b.snippet_title,
    a.region,
    a.views,
    a.likes
FROM db_youtube_cleaned.raw_statistics a
INNER JOIN db_youtube_cleaned.cleaned_statistics_reference_data b
    ON a.category_id = b.id
WHERE a.region = 'ca'
LIMIT 20;

-- 5. Preview final analytics table
SELECT *
FROM db_youtube_analytics.final_analytics
LIMIT 10;

-- 6. Query final analytics table for a specific region
SELECT *
FROM db_youtube_analytics.final_analytics
WHERE region = 'ca'
LIMIT 20;

-- 7. Aggregate total views by category
SELECT
    snippet_title AS category_name,
    SUM(views) AS total_views
FROM db_youtube_analytics.final_analytics
GROUP BY snippet_title
ORDER BY total_views DESC;

-- 8. Aggregate total likes by category
SELECT
    snippet_title AS category_name,
    SUM(likes) AS total_likes
FROM db_youtube_analytics.final_analytics
GROUP BY snippet_title
ORDER BY total_likes DESC;

-- 9. Aggregate total views by region
SELECT
    region,
    SUM(views) AS total_views
FROM db_youtube_analytics.final_analytics
GROUP BY region
ORDER BY total_views DESC;

-- =====================================================
-- 10. Before optimization: query on cleaned tables
-- Runtime: 8.547 seconds
-- =====================================================
SELECT *
FROM db_youtube_cleaned.raw_statistics a
INNER JOIN db_youtube_cleaned.cleaned_statistics_reference_data b
    ON a.category_id = b.id
WHERE a.region = 'ca';

-- =====================================================
-- 11. After optimization: query on final analytics table
-- Runtime: 1.416 seconds
-- =====================================================

SELECT *
FROM db_youtube_analytics.final_analytics
WHERE region = 'ca';


-- 12. Business query: Most viewed categories in Canada
SELECT
    snippet_title AS category_name,
    SUM(views) AS total_views
FROM db_youtube_analytics.final_analytics
WHERE region = 'ca'
GROUP BY snippet_title
ORDER BY total_views DESC;

-- 13. Business query: most liked categories by region
SELECT
    region,
    snippet_title AS category_name,
    SUM(likes) AS total_likes
FROM db_youtube_analytics.final_analytics
GROUP BY region, snippet_title
ORDER BY region, total_likes DESC;