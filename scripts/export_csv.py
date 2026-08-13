"""
export_for_powerbi.py

หน้าที่: export ข้อมูลจาก data/netflix.duckdb ออกมาเป็นไฟล์ CSV
         สำหรับให้ Power BI import ไปทำ dashboard แบบ static (ไม่ใช่ live connection)

Export ออกมา 4 ไฟล์ที่ data/powerbi_export/:
1. titles.csv           - fact table หลัก (1 แถว = 1 เรื่อง)
2. genres.csv            - bridge table show_id <-> genre (มีอยู่แล้วจากตอน transform)
3. title_countries.csv   - bridge table show_id <-> country (แตกจากคอลัมน์ country ที่มีหลายค่าคั่น comma)
4. yearly_summary.csv    - สรุปตัวชี้วัดรายปี (มาจาก VIEW ที่สร้างไว้ใน analysis.sql, ข้ามถ้ายังไม่เคยรัน)

หมายเหตุ: สร้าง title_countries.csv แยกต่างหาก เพราะคอลัมน์ country ในตาราง titles
อาจมีหลายประเทศคั่นด้วย comma ในแถวเดียว (เช่น "United States, France") ถ้า import
titles.csv ตรงๆ เข้า Power BI แล้วทำแผนที่ตามประเทศจะนับผิด ต้องใช้ตารางนี้แทน
"""

import duckdb
from pathlib import Path

# ---------------------------------------------------------------------------
# ค่าคงที่ของพาธไฟล์ (default)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "netflix.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "powerbi_export"


def export_for_powerbi(db_path: Path = DB_PATH, export_dir: Path = EXPORT_DIR) -> None:
    """
    Export ตารางและ view ที่จำเป็นทั้งหมดออกมาเป็น CSV สำหรับ Power BI

    Parameters
    ----------
    db_path : Path
        พาธของไฟล์ฐานข้อมูล DuckDB ต้นทาง
    export_dir : Path
        โฟลเดอร์ปลายทางที่จะเก็บไฟล์ CSV ทั้งหมด (ต้องสร้างเองก่อนรัน ไม่ auto-create)
    """
    if not export_dir.is_dir():
        raise FileNotFoundError(
            f"ไม่พบโฟลเดอร์ปลายทาง: {export_dir} — กรุณาสร้างโฟลเดอร์นี้ก่อนรัน script"
        )

    # เปิดแบบ read_only=True เพราะแค่จะอ่านออกไปเป็นไฟล์ ไม่แก้ไขข้อมูลใน DuckDB เลย
    con = duckdb.connect(str(db_path), read_only=True)

    # ใช้คำสั่ง COPY ... TO ของ DuckDB โดยตรง (เร็วกว่าดึงเข้า pandas DataFrame แล้วค่อย to_csv)
    con.execute(f"""
        COPY (SELECT * FROM titles)
        TO '{export_dir / "titles.csv"}' (HEADER, DELIMITER ',')
    """)

    con.execute(f"""
        COPY (SELECT * FROM genres)
        TO '{export_dir / "genres.csv"}' (HEADER, DELIMITER ',')
    """)

    # แตกคอลัมน์ country ที่มีหลายค่า ให้เป็น bridge table แบบเดียวกับ genres
    con.execute(f"""
        COPY (
            SELECT show_id, TRIM(UNNEST(STRING_SPLIT(country, ','))) AS country
            FROM titles
            WHERE country != 'Unknown'
        )
        TO '{export_dir / "title_countries.csv"}' (HEADER, DELIMITER ',')
    """)

    # เช็คก่อนว่า view yearly_summary ถูกสร้างไว้แล้วหรือยัง (มาจากตอนรัน analysis.sql)
    # ถ้ายังไม่เคยรัน ข้ามไปเฉยๆ ไม่ error เพราะไฟล์อื่นยัง export ได้ตามปกติ
    view_exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'yearly_summary'"
    ).fetchone()[0]

    if view_exists:
        con.execute(f"""
            COPY (SELECT * FROM yearly_summary)
            TO '{export_dir / "yearly_summary.csv"}' (HEADER, DELIMITER ',')
        """)
    else:
        print("[export] ข้าม yearly_summary.csv เพราะยังไม่พบ VIEW นี้ (ลองรัน analysis.sql ก่อน)")

    con.close()
    print(f"[export] เสร็จเรียบร้อย ไฟล์ทั้งหมดอยู่ที่ {export_dir}")


if __name__ == "__main__":
    export_for_powerbi()
