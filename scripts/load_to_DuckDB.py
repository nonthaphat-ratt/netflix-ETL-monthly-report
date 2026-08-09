"""
load_csv.py

หน้าที่: โหลดไฟล์ที่ transform แล้ว (รายเดือน) เข้า DuckDB
         สร้าง 2 ตาราง: titles (ข้อมูลหลัก) และ genres (ความสัมพันธ์ show_id <-> genre)

หลักการที่ยึดตาม (เหมือน extract_csv.py และ transform_csv.py):
- Airflow-native -> รับ logical_date เป็นพารามิเตอร์ ไม่ hardcode เดือน
- Idempotent     -> ใช้วิธี "delete-then-insert" คือลบข้อมูลของ show_id ที่กำลังจะโหลด
                    ทิ้งก่อน แล้วค่อย insert ใหม่ ป้องกันข้อมูลซ้ำเวลารันซ้ำเดือนเดิม (เช่น retry)
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# ค่าคงที่ของพาธไฟล์ (default)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRANSFORMED_DIR = PROJECT_ROOT / "data" / "transformed"
GENRE_DIR = PROJECT_ROOT / "data" / "genres"
DB_PATH = PROJECT_ROOT / "data" / "netflix.duckdb"


def load_monthly_data(
    logical_date: datetime,
    transformed_dir: Path = TRANSFORMED_DIR,
    genre_dir: Path = GENRE_DIR,
    db_path: Path = DB_PATH,
) -> None:
    """
    โหลดข้อมูลของเดือนที่ระบุ (อ่านจากไฟล์ transform) เข้า DuckDB

    Parameters
    ----------
    logical_date : datetime
        ค่าที่ Airflow ส่งมาให้ในแต่ละรอบ ใช้หาว่าต้องอ่านไฟล์ transform ของเดือนไหน
    transformed_dir : Path
        โฟลเดอร์ที่เก็บไฟล์หลักที่ clean แล้ว (จาก transform_csv.py)
    genre_dir : Path
        โฟลเดอร์ที่เก็บไฟล์ตาราง show_id <-> genre (จาก transform_csv.py)
    db_path : Path
        พาธของไฟล์ฐานข้อมูล DuckDB (ถ้ายังไม่มีไฟล์ DuckDB จะถูกสร้างใหม่อัตโนมัติ)
    """
    target_year = logical_date.year
    target_month = logical_date.month
    file_name = f"{target_year:04d}-{target_month:02d}.csv"

    main_path = transformed_dir / file_name
    genre_path = genre_dir / file_name

    # เช็คว่าไฟล์ input มีอยู่จริงก่อน — ถ้าไม่มี แปลว่ายังไม่ได้รัน transform ของเดือนนี้
    if not main_path.is_file():
        raise FileNotFoundError(f"ไม่พบไฟล์: {main_path} — กรุณารัน transform step ก่อน")
    if not genre_path.is_file():
        raise FileNotFoundError(f"ไม่พบไฟล์: {genre_path} — กรุณารัน transform step ก่อน")

    titles_df = pd.read_csv(main_path)
    genres_df = pd.read_csv(genre_path)

    # เชื่อมต่อ DuckDB (ถ้าไฟล์ตามพาธนี้ยังไม่มี DuckDB จะสร้างไฟล์ใหม่ให้อัตโนมัติ)
    con = duckdb.connect(str(db_path))

    try:
        # -----------------------------------------------------------------
        # สร้างตาราง (รันได้ซ้ำหลายครั้งอย่างปลอดภัย เพราะมี IF NOT EXISTS)
        # หมายเหตุ: คอลัมน์ "cast" ต้องอยู่ในเครื่องหมาย " " เพราะ cast เป็นคำสงวนใน SQL
        # (ใช้เป็นฟังก์ชันแปลงชนิดข้อมูล เช่น CAST(x AS INTEGER)) ถ้าไม่ quote จะ error ทันที
        # -----------------------------------------------------------------
        con.execute("""
            CREATE TABLE IF NOT EXISTS titles (
                show_id VARCHAR,
                type VARCHAR,
                title VARCHAR,
                director VARCHAR,
                "cast" VARCHAR,
                country VARCHAR,
                date_added DATE,
                release_year INTEGER,
                rating VARCHAR,
                duration_minutes DOUBLE,
                duration_seasons DOUBLE
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                show_id VARCHAR,
                genre VARCHAR
            )
        """)

        # -----------------------------------------------------------------
        # ลงทะเบียน DataFrame ให้ DuckDB มองเห็นเป็นเหมือน "ตารางชั่วคราว"
        # เพื่อให้เขียน SQL query อ้างอิงชื่อ titles_df / genres_df ได้ตรงๆ
        # -----------------------------------------------------------------
        con.register("titles_df", titles_df)
        con.register("genres_df", genres_df)

        # -----------------------------------------------------------------
        # Idempotent load: ลบข้อมูลของ show_id ที่กำลังจะโหลดทิ้งก่อน (ถ้ามีอยู่แล้ว)
        # แล้วค่อย insert ใหม่ทับ วิธีนี้ทำให้รันซ้ำเดือนเดิมกี่ครั้งก็ได้ผลลัพธ์เดิมเสมอ
        # (เช่น Airflow retry รอบเดิมซ้ำ ข้อมูลจะไม่ถูก insert ซ้ำเป็น 2 ชุด)
        # -----------------------------------------------------------------
        show_ids_this_month = titles_df["show_id"].tolist()
        con.execute("DELETE FROM titles WHERE show_id = ANY(?)", [show_ids_this_month])
        con.execute("DELETE FROM genres WHERE show_id = ANY(?)", [show_ids_this_month])

        con.execute("INSERT INTO titles SELECT * FROM titles_df")
        con.execute("INSERT INTO genres SELECT * FROM genres_df")

        print(
            f"[load] เดือน {target_year:04d}-{target_month:02d}: "
            f"titles {len(titles_df)} แถว, genres {len(genres_df)} แถว -> {db_path}"
        )
    finally:
        # ปิดการเชื่อมต่อเสมอ ไม่ว่าจะสำเร็จหรือ error ระหว่างทาง
        con.close()


# ---------------------------------------------------------------------------
# ส่วนทดสอบแบบ standalone (รันตรงๆ ด้วย python load_csv.py โดยไม่ผ่าน Airflow)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_logical_date = datetime(2021, 9, 1)
    load_monthly_data(logical_date=test_logical_date)