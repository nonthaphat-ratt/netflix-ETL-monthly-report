-- ============================================================================
-- Netflix Content Library — SQL Analysis
-- ============================================================================
-- Database: data/netflix.duckdb
-- Tables:
--   titles  (1 แถว = 1 เรื่อง) : show_id, type, title, director, "cast", country,
--                                 date_added, release_year, rating,
--                                 duration_minutes, duration_seasons
--   genres  (1 แถว = 1 genre ของ 1 เรื่อง, normalize จาก listed_in) : show_id, genre
--
-- วิธีรัน:
--   - VSCode DuckDB extension: เปิด Command Palette (Ctrl+Shift+P) พิมพ์ DuckDB: Selct Database แล้วเลือก data/netflix.duckdb
--
-- หมายเหตุสำคัญเกี่ยวกับข้อมูล:
--   - คอลัมน์ "cast" ต้องใส่เครื่องหมาย " " ครอบเสมอ เพราะเป็นคำสงวนใน SQL
--   - คอลัมน์ country และ director/cast อาจมีค่าหลายค่าคั่นด้วย comma ในแถวเดียว
--     (เช่น "United States, France, United Kingdom") ต้อง SPLIT ก่อนนับถ้าต้องการความแม่นยำ
--   - genres ถูกแยกตารางไว้แล้วตั้งแต่ตอน transform จึงไม่ต้อง split ซ้ำ
-- ============================================================================


-- ============================================================================
-- SECTION A: ภาพรวมข้อมูล & ตรวจสุขภาพ pipeline (Data Overview & Quality)
-- ============================================================================

-- [A1 | Basic]
-- โจทย์: ภาพรวมคลังคอนเทนต์ทั้งหมดมีกี่เรื่อง แบ่งเป็น Movie/TV Show เท่าไหร่
-- และข้อมูลครอบคลุมช่วงเวลาไหนบ้าง — ใช้เป็นสไลด์แรกของทุกรายงาน
SELECT
    COUNT(*) AS total_titles,
    COUNT(*) FILTER (WHERE type = 'Movie') AS total_movies,
    COUNT(*) FILTER (WHERE type = 'TV Show') AS total_tv_shows,
    MIN(date_added) AS earliest_date_added,
    MAX(date_added) AS latest_date_added
FROM titles;


-- [A2 | Advanced]
-- โจทย์: pipeline สุขภาพดีไหม? เช็คสัดส่วนข้อมูลที่หลุดไปเป็น "Unknown"/"Not Rated"
-- ตอน clean ข้อมูล (จากค่า null เดิม) — ควรรันทุกครั้งหลัง pipeline โหลดข้อมูลจบ
-- เพื่อ monitor คุณภาพข้อมูลไม่ให้แย่ลงเรื่อยๆ โดยไม่รู้ตัว
SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE director = 'Unknown') / COUNT(*), 2) AS pct_unknown_director,
    ROUND(100.0 * COUNT(*) FILTER (WHERE "cast" = 'Unknown') / COUNT(*), 2)     AS pct_unknown_cast,
    ROUND(100.0 * COUNT(*) FILTER (WHERE country = 'Unknown') / COUNT(*), 2)   AS pct_unknown_country,
    ROUND(100.0 * COUNT(*) FILTER (WHERE rating = 'Not Rated') / COUNT(*), 2)  AS pct_not_rated
FROM titles;


-- ============================================================================
-- SECTION B: การเติบโตของคลังคอนเทนต์ (Content Growth & Library Trends)
-- ============================================================================

-- [B1 | Basic]
-- โจทย์: แต่ละปี Netflix เพิ่ม title เข้าคลังกี่เรื่อง — กราฟเส้นพื้นฐานที่สุด
SELECT
    EXTRACT(YEAR FROM date_added) AS year_added,
    COUNT(*) AS titles_added
FROM titles
GROUP BY year_added
ORDER BY year_added;


-- [B2 | Advanced]
-- โจทย์: คลังคอนเทนต์โตสะสม (cumulative) ไปถึงไหนแล้วในแต่ละปี
-- และแต่ละปีโตขึ้น/ลดลงกี่ % เทียบปีก่อนหน้า (YoY growth)
-- ใช้ window function SUM()/LAG() แทนการ query วนหลายรอบ
WITH yearly AS (
    SELECT
        EXTRACT(YEAR FROM date_added) AS year_added,
        COUNT(*) AS titles_added
    FROM titles
    GROUP BY year_added
)
SELECT
    year_added,
    titles_added,
    SUM(titles_added) OVER (ORDER BY year_added) AS cumulative_titles,
    ROUND(
        100.0 * (titles_added - LAG(titles_added) OVER (ORDER BY year_added))
        / NULLIF(LAG(titles_added) OVER (ORDER BY year_added), 0),
        1
    ) AS yoy_growth_pct
FROM yearly
ORDER BY year_added;


-- [B3 | Advanced]
-- โจทย์: กลยุทธ์เนื้อหาของ Netflix เอียงไปทาง TV Show มากขึ้นไหมเมื่อเวลาผ่านไป
-- (อุตสาหกรรมสตรีมมิงมีเทรนด์ที่ผู้เล่นหลายเจ้าหันไปเน้น series เพื่อ "รั้ง" คนดูให้อยู่นาน)
SELECT
    EXTRACT(YEAR FROM date_added) AS year_added,
    COUNT(*) FILTER (WHERE type = 'Movie') AS movies,
    COUNT(*) FILTER (WHERE type = 'TV Show') AS tv_shows,
    ROUND(100.0 * COUNT(*) FILTER (WHERE type = 'TV Show') / COUNT(*), 1) AS tv_show_share_pct
FROM titles
GROUP BY year_added
ORDER BY year_added;


-- ============================================================================
-- SECTION C: ความยาวคอนเทนต์ (Duration Analysis)
-- ============================================================================

-- [C1 | Basic]
-- โจทย์: หนังใน Netflix ยาวเฉลี่ยกี่นาที สั้นสุด/ยาวสุดกี่นาที
SELECT
    ROUND(AVG(duration_minutes), 1) AS avg_minutes,
    MIN(duration_minutes) AS min_minutes,
    MAX(duration_minutes) AS max_minutes
FROM titles
WHERE type = 'Movie';


-- [C2 | Advanced]
-- โจทย์: หนังยุคใหม่ (ตาม release_year) สั้นลงหรือยาวขึ้นเมื่อเทียบกับหนังยุคเก่า
-- (เทรนด์อุตสาหกรรมภาพยนตร์ที่มักถูกพูดถึงว่าคนดูยุคสตรีมมิงมีสมาธิสั้นลง)
SELECT
    release_year,
    COUNT(*) AS movie_count,
    ROUND(AVG(duration_minutes), 1) AS avg_duration_minutes
FROM titles
WHERE type = 'Movie' AND release_year >= 2000
GROUP BY release_year
ORDER BY release_year;


-- [C3 | Advanced]
-- โจทย์: TV Show ใน Netflix ส่วนใหญ่เป็น "limited series" (จบใน 1 season)
-- หรือเป็นซีรีส์ยาวที่ต้องลงทุนต่อเนื่องหลาย season — ช่วยวางแผนงบลงทุนคอนเทนต์
SELECT
    CASE
        WHEN duration_seasons = 1            THEN '1) Limited series (1 season)'
        WHEN duration_seasons BETWEEN 2 AND 4 THEN '2) Short-run (2-4 seasons)'
        WHEN duration_seasons >= 5           THEN '3) Long-running (5+ seasons)'
    END AS series_length_group,
    COUNT(*) AS show_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_tv_shows
FROM titles
WHERE type = 'TV Show'
GROUP BY series_length_group
ORDER BY series_length_group;


-- ============================================================================
-- SECTION D: วิเคราะห์ Genre (Genre Analysis)
-- ============================================================================

-- [D1 | Basic]
-- โจทย์: genre ไหนมี title เยอะที่สุดในคลัง Top 10
SELECT
    genre,
    COUNT(*) AS title_count
FROM genres
GROUP BY genre
ORDER BY title_count DESC
LIMIT 10;


-- [D2 | Advanced]
-- โจทย์: genre ไหน "กำลังมาแรง" จริงๆ ไม่ใช่แค่เยอะโดยรวม — เทียบสัดส่วนช่วงปี 2016-2018
-- กับ 2019-2021 หา genre ที่เติบโตเร็วที่สุด (มีประโยชน์ต่อการวางแผน content สั่งซื้อใหม่)
WITH genre_period AS (
    SELECT
        g.genre,
        CASE
            WHEN EXTRACT(YEAR FROM t.date_added) BETWEEN 2016 AND 2018 THEN 'period_2016_2018'
            WHEN EXTRACT(YEAR FROM t.date_added) BETWEEN 2019 AND 2021 THEN 'period_2019_2021'
        END AS period
    FROM genres g
    JOIN titles t ON g.show_id = t.show_id
    WHERE EXTRACT(YEAR FROM t.date_added) BETWEEN 2016 AND 2021
),
pivoted AS (
    SELECT
        genre,
        COUNT(*) FILTER (WHERE period = 'period_2016_2018') AS count_2016_2018,
        COUNT(*) FILTER (WHERE period = 'period_2019_2021') AS count_2019_2021
    FROM genre_period
    GROUP BY genre
)
SELECT
    genre,
    count_2016_2018,
    count_2019_2021,
    ROUND(100.0 * (count_2019_2021 - count_2016_2018) / NULLIF(count_2016_2018, 0), 1) AS growth_pct
FROM pivoted
WHERE count_2016_2018 >= 20   -- กรองฐานเล็กเกินไปทิ้ง กัน % เพี้ยนจากตัวเลขน้อยๆ
ORDER BY growth_pct DESC
LIMIT 10;


-- [D3 | Advanced]
-- โจทย์: genre คู่ไหนมักถูกแปะรวมกันในเรื่องเดียวบ่อยที่สุด (cross-genre pairing)
-- ใช้ self-join บนตาราง genres — เป็นแนวทางเดียวกับที่ระบบแนะนำหนัง (recommendation) ใช้จริง
SELECT
    g1.genre AS genre_a,
    g2.genre AS genre_b,
    COUNT(*) AS titles_with_both
FROM genres g1
JOIN genres g2
    ON g1.show_id = g2.show_id
    AND g1.genre < g2.genre        -- กัน pair ซ้ำ (A,B)/(B,A) และกัน self-pair (A,A)
GROUP BY g1.genre, g2.genre
ORDER BY titles_with_both DESC
LIMIT 10;


-- ============================================================================
-- SECTION E: วิเคราะห์ตามภูมิศาสตร์ (Geographic Analysis)
-- ============================================================================

-- [E1 | Basic]
-- โจทย์: ประเทศไหนมี title เยอะที่สุด (นับแบบง่าย: เอาแค่ประเทศแรกที่ระบุในแต่ละแถว)
SELECT
    SPLIT_PART(country, ',', 1) AS primary_country,
    COUNT(*) AS title_count
FROM titles
WHERE country != 'Unknown'
GROUP BY primary_country
ORDER BY title_count DESC
LIMIT 10;


-- [E2 | Advanced]
-- โจทย์: เหมือน E1 แต่แม่นยำกว่า — เรื่องที่ร่วมผลิตหลายประเทศ (เช่น "Spain, Mexico, France")
-- จะถูกนับให้ครบทุกประเทศ ไม่ใช่แค่ประเทศแรก (สำคัญมากถ้าจะใช้ตัวเลขนี้ไปตัดสินใจจริง)
WITH country_split AS (
    SELECT
        show_id,
        TRIM(UNNEST(STRING_SPLIT(country, ','))) AS single_country
    FROM titles
    WHERE country != 'Unknown'
)
SELECT
    single_country,
    COUNT(*) AS title_count
FROM country_split
GROUP BY single_country
ORDER BY title_count DESC
LIMIT 10;


-- [E3 | Advanced]
-- โจทย์: สัดส่วนคอนเทนต์นอกสหรัฐฯ เพิ่มขึ้นตามปีไหม — สะท้อนกลยุทธ์ localization/global expansion
-- ของ Netflix ได้ตรงจุดกว่าดูแค่ยอดรวม เพราะเห็น "ทิศทาง" การเปลี่ยนแปลง
WITH country_split AS (
    SELECT
        t.show_id,
        t.date_added,
        TRIM(UNNEST(STRING_SPLIT(t.country, ','))) AS single_country
    FROM titles t
    WHERE t.country != 'Unknown'
)
SELECT
    EXTRACT(YEAR FROM date_added) AS year_added,
    COUNT(DISTINCT show_id) FILTER (WHERE single_country = 'United States')  AS us_titles,
    COUNT(DISTINCT show_id) FILTER (WHERE single_country != 'United States') AS non_us_titles,
    ROUND(
        100.0 * COUNT(DISTINCT show_id) FILTER (WHERE single_country != 'United States')
        / COUNT(DISTINCT show_id),
        1
    ) AS non_us_share_pct
FROM country_split
GROUP BY year_added
ORDER BY year_added;


-- ============================================================================
-- SECTION F: Rating & กลุ่มผู้ชม (Rating & Audience Analysis)
-- ============================================================================

-- [F1 | Basic]
-- โจทย์: คอนเทนต์ทั้งหมดแบ่งตาม rating เป็นสัดส่วนเท่าไหร่บ้าง
SELECT
    rating,
    COUNT(*) AS title_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM titles
GROUP BY rating
ORDER BY title_count DESC;


-- [F2 | Advanced]
-- โจทย์: Netflix เพิ่มคอนเทนต์ "ผู้ใหญ่" (TV-MA/R/NC-17) มากขึ้นเรื่อยๆ เทียบกับคอนเทนต์
-- "ครอบครัว" (G/PG/TV-G/TV-Y/TV-Y7) หรือไม่ — บ่งบอกทิศทางกลุ่มเป้าหมายหลักของแพลตฟอร์ม
WITH classified AS (
    SELECT
        EXTRACT(YEAR FROM date_added) AS year_added,
        CASE
            WHEN rating IN ('TV-MA', 'R', 'NC-17') THEN 'mature'
            WHEN rating IN ('G', 'PG', 'TV-G', 'TV-Y', 'TV-Y7', 'TV-Y7-FV') THEN 'family'
            ELSE 'other'
        END AS audience_group
    FROM titles
)
SELECT
    year_added,
    COUNT(*) FILTER (WHERE audience_group = 'mature') AS mature_count,
    COUNT(*) FILTER (WHERE audience_group = 'family') AS family_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE audience_group = 'mature') / COUNT(*), 1) AS mature_share_pct
FROM classified
GROUP BY year_added
ORDER BY year_added;


-- ============================================================================
-- SECTION G: กลยุทธ์การจัดหาคอนเทนต์ (Content Acquisition Strategy / Freshness)
-- ============================================================================

-- [G1 | Basic]
-- โจทย์: โดยเฉลี่ยแล้ว Netflix เอาคอนเทนต์เข้าคลังหลังจากที่มันฉาย/ผลิตเสร็จไปแล้วกี่ปี
-- ("acquisition lag" — ตัวเลขนี้บอกว่า Netflix เน้นคอนเทนต์ใหม่ทันกระแส หรือเน้นคลังเก่า)
SELECT
    ROUND(AVG(EXTRACT(YEAR FROM date_added) - release_year), 1) AS avg_acquisition_lag_years
FROM titles;


-- [G2 | Advanced]
-- โจทย์: สัดส่วน "คอนเทนต์ใหม่ทันฉาย" (lag <= 1 ปี) เทียบกับ "คลังเก่า" (lag >= 5 ปี)
-- เปลี่ยนไปยังไงตามปี — ถ้าสัดส่วนคอนเทนต์ใหม่เพิ่มขึ้นเรื่อยๆ แปลว่า Netflix ลงทุน
-- original/day-and-date content มากขึ้น ไม่ได้พึ่งคลังเก่าเป็นหลักเหมือนช่วงแรก
WITH lag_calc AS (
    SELECT
        EXTRACT(YEAR FROM date_added) AS year_added,
        EXTRACT(YEAR FROM date_added) - release_year AS acquisition_lag
    FROM titles
    WHERE release_year IS NOT NULL
)
SELECT
    year_added,
    COUNT(*) FILTER (WHERE acquisition_lag <= 1) AS fresh_content,
    COUNT(*) FILTER (WHERE acquisition_lag >= 5) AS library_content,
    ROUND(100.0 * COUNT(*) FILTER (WHERE acquisition_lag <= 1) / COUNT(*), 1) AS fresh_content_pct
FROM lag_calc
GROUP BY year_added
ORDER BY year_added;


-- ============================================================================
-- SECTION H: วิเคราะห์บุคลากร (Director / Cast Analysis)
-- ============================================================================

-- [H1 | Basic]
-- โจทย์: ผู้กำกับคนไหนมีผลงานอยู่ใน Netflix เยอะที่สุด Top 10 (ไม่นับ Unknown)
SELECT
    director,
    COUNT(*) AS title_count
FROM titles
WHERE director != 'Unknown'
GROUP BY director
ORDER BY title_count DESC
LIMIT 10;


-- [H2 | Advanced]
-- โจทย์: นักแสดงคนไหนปรากฏตัวในคลัง Netflix บ่อยที่สุด — ต้องแตกคอลัมน์ "cast" ที่มีชื่อ
-- หลายคนคั่นด้วย comma ในแถวเดียวก่อน (เหมือนที่แตก country ใน E2) ถึงจะนับถูกคน
WITH cast_split AS (
    SELECT
        show_id,
        TRIM(UNNEST(STRING_SPLIT("cast", ','))) AS actor
    FROM titles
    WHERE "cast" != 'Unknown'
)
SELECT
    actor,
    COUNT(*) AS title_count
FROM cast_split
GROUP BY actor
ORDER BY title_count DESC
LIMIT 10;


-- [H3 | Advanced]
-- โจทย์: ผู้กำกับคนไหน "หลากหลายแนวที่สุด" (ทำงานครอบคลุมหลาย genre) ไม่ใช่แค่ทำเยอะเรื่อง
-- แต่ทำเรื่องเดิมๆ แนวเดียว — กรองเฉพาะคนที่มีผลงาน 5 เรื่องขึ้นไปกันฐานเล็กเกินไป
WITH director_split AS (
    SELECT
        show_id,
        TRIM(UNNEST(STRING_SPLIT(director, ','))) AS single_director
    FROM titles
    WHERE director != 'Unknown'
)
SELECT
    ds.single_director,
    COUNT(DISTINCT ds.show_id) AS title_count,
    COUNT(DISTINCT g.genre) AS distinct_genre_count
FROM director_split ds
JOIN genres g ON ds.show_id = g.show_id
GROUP BY ds.single_director
HAVING COUNT(DISTINCT ds.show_id) >= 5
ORDER BY distinct_genre_count DESC, title_count DESC
LIMIT 10;


-- ============================================================================
-- SECTION I: ความเป็นฤดูกาล (Seasonality)
-- ============================================================================

-- [I1 | Basic]
-- โจทย์: เดือนไหน (ม.ค.-ธ.ค.) ที่ Netflix เพิ่มคอนเทนต์เยอะที่สุดโดยเฉลี่ย (รวมทุกปี)
SELECT
    EXTRACT(MONTH FROM date_added) AS month_number,
    COUNT(*) AS titles_added
FROM titles
GROUP BY month_number
ORDER BY month_number;


-- [I2 | Advanced]
-- โจทย์: แต่ละไตรมาส (Q1-Q4) มี pattern การเพิ่ม Movie vs TV Show ต่างกันไหม
-- (เผื่อพบว่า Netflix มักปล่อย TV Show ช่วงปลายปีเพื่อรับเทศกาล เป็นต้น)
SELECT
    'Q' || CAST(CEIL(EXTRACT(MONTH FROM date_added) / 3.0) AS INTEGER) AS quarter,
    COUNT(*) FILTER (WHERE type = 'Movie')   AS movies,
    COUNT(*) FILTER (WHERE type = 'TV Show') AS tv_shows,
    COUNT(*) AS total
FROM titles
GROUP BY quarter
ORDER BY quarter;


-- ============================================================================
-- SECTION J: Executive Summary View (พร้อมใช้งานต่อทันที)
-- ============================================================================

-- [J1 | Advanced]
-- โจทย์: รวมตัวชี้วัดสำคัญของแต่ละปีไว้ในที่เดียว (จำนวน title, สัดส่วน Movie/TV,
-- ความยาวหนังเฉลี่ย, ความหลากหลายประเทศ, acquisition lag) แล้วเซฟเป็น VIEW
-- เพื่อให้คนอื่น (เช่นทีม BI) SELECT ต่อได้ทันทีโดยไม่ต้องเขียน query ยาวซ้ำทุกครั้ง
-- หมายเหตุ: รันครั้งเดียวพอ (VIEW จะถูกบันทึกไว้ใน netflix.duckdb ถาวร)
CREATE OR REPLACE VIEW yearly_summary AS
WITH country_split AS (
    SELECT show_id, TRIM(UNNEST(STRING_SPLIT(country, ','))) AS single_country
    FROM titles WHERE country != 'Unknown'
)
SELECT
    EXTRACT(YEAR FROM t.date_added) AS year_added,
    COUNT(DISTINCT t.show_id) AS total_titles,
    COUNT(DISTINCT t.show_id) FILTER (WHERE t.type = 'Movie') AS movies,
    COUNT(DISTINCT t.show_id) FILTER (WHERE t.type = 'TV Show') AS tv_shows,
    ROUND(AVG(t.duration_minutes), 1) AS avg_movie_minutes,
    COUNT(DISTINCT cs.single_country) AS distinct_countries,
    ROUND(AVG(EXTRACT(YEAR FROM t.date_added) - t.release_year), 1) AS avg_acquisition_lag_years
FROM titles t
LEFT JOIN country_split cs ON t.show_id = cs.show_id
GROUP BY year_added
ORDER BY year_added;


-- [J2 | Basic]
-- โจทย์: ตัวอย่างการเรียกใช้ VIEW ที่สร้างไว้ใน J1 — ดู 5 ปีล่าสุด
-- (ต่อไปใครจะ query สรุปรายปี ไม่ต้อง copy query ยาวๆ ซ้ำ แค่ SELECT จาก view นี้พอ)
SELECT * FROM yearly_summary ORDER BY year_added DESC LIMIT 5;
