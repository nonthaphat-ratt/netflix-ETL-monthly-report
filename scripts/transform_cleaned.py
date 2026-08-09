"""
transform_csv.py

หน้าที่: รับไฟล์ที่ extract ออกมาแล้ว (รายเดือน) มาทำความสะอาดข้อมูล แล้วแยก output
         ออกเป็น 2 ไฟล์ ตามที่ออกแบบไว้:

         1) ไฟล์หลัก (data/transformed/{yyyy-mm}.csv)
            เก็บข้อมูลของแต่ละเรื่อง 1 แถวต่อ 1 เรื่อง (clean แล้ว)

         2) ไฟล์ genre (data/genres/{yyyy-mm}.csv)
            เก็บความสัมพันธ์ show_id <-> genre แบบ 1 ต่อหลาย (normalize จาก listed_in)

หลักการที่ยึดตาม (เหมือน extract_csv.py):
- Airflow-native -> รับ logical_date เป็นพารามิเตอร์ ไม่ hardcode เดือน
- Idempotent     -> เขียนทับไฟล์เดิมเสมอ รันซ้ำกี่ครั้งก็ได้ผลลัพธ์เหมือนเดิม
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# ค่าคงที่ของพาธไฟล์ (default)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXTRACTED_DIR = PROJECT_ROOT / "data" / "extracted"
TRANSFORMED_DIR = PROJECT_ROOT / "data" / "transformed"
GENRE_DIR = PROJECT_ROOT / "data" / "genres"

# รายชื่อ rating มาตรฐานของ Netflix ใช้เช็คว่าค่าไหน "หลุด format" (เกิดจาก column-shift ตอน EDA)
STANDARD_RATINGS = {
    "G", "PG", "PG-13", "R", "NC-17", "NR", "UR",
    "TV-Y", "TV-Y7", "TV-Y7-FV", "TV-G", "TV-PG", "TV-14", "TV-MA",
}


def transform_monthly_data(
    logical_date: datetime,
    extracted_dir: Path = EXTRACTED_DIR,
    transformed_dir: Path = TRANSFORMED_DIR,
    genre_dir: Path = GENRE_DIR,
) -> tuple[Path, Path]:
    """
    Clean ข้อมูลของเดือนที่ระบุ (อ่านจากไฟล์ extract) แล้วเขียนออกเป็น 2 ไฟล์

    Parameters
    ----------
    logical_date : datetime
        ค่าที่ Airflow ส่งมาให้ในแต่ละรอบ ใช้หาว่าต้องอ่านไฟล์ extract ของเดือนไหน
    extracted_dir : Path
        โฟลเดอร์ที่เก็บไฟล์ผลลัพธ์จากขั้นตอน extract
    transformed_dir : Path
        โฟลเดอร์ปลายทางของไฟล์หลักที่ clean แล้ว
    genre_dir : Path
        โฟลเดอร์ปลายทางของไฟล์ตารางความสัมพันธ์ show_id <-> genre

    Returns
    -------
    tuple[Path, Path]
        พาธของ (ไฟล์หลัก, ไฟล์ genre) ที่เขียนออกมาในรอบนี้
    """
    target_year = logical_date.year
    target_month = logical_date.month
    file_name = f"{target_year:04d}-{target_month:02d}.csv"

    # อ่านไฟล์ที่ extract ไว้ของเดือนนี้ (ไฟล์เดียวกับที่ extract_csv.py เขียนออกมา)
    df = pd.read_csv(extracted_dir / file_name)

    # -------------------------------------------------------------------
    # ขั้นที่ 1: แก้ปัญหา column-shift ที่เจอตอน EDA
    # บางแถวค่าที่ควรอยู่ใน duration (เช่น "74 min") ดันไปโผล่ใน rating แทน
    # เช็คจาก pattern: เป็นตัวเลขตามด้วย min/Season/Seasons
    # -------------------------------------------------------------------
    shifted_mask = df["rating"].str.match(r"^\d+\s*(min|Seasons?)$", na=False)

    # ย้ายค่าที่หลุดกลับไปที่ duration (ของแถวเหล่านี้ duration เดิมเป็นค่าว่างอยู่แล้ว)
    df.loc[shifted_mask, "duration"] = df.loc[shifted_mask, "rating"]
    # เคลียร์ rating ของแถวที่หลุด ให้เป็นค่าว่าง (จะถูกเติม "Not Rated" พร้อม null ปกติในขั้นถัดไป)
    df.loc[shifted_mask, "rating"] = pd.NA

    # -------------------------------------------------------------------
    # ขั้นที่ 2: เติมค่า null ให้คอลัมน์ข้อความ
    # -------------------------------------------------------------------
    df["director"] = df["director"].fillna("Unknown")
    df["cast"] = df["cast"].fillna("Unknown")
    df["country"] = df["country"].fillna("Unknown")
    df["rating"] = df["rating"].fillna("Not Rated")

    # -------------------------------------------------------------------
    # ขั้นที่ 3: แปลง date_added เป็นรูปแบบวันที่มาตรฐาน (YYYY-MM-DD)
    # หมายเหตุ: แถวที่ date_added เป็น null จะไม่ถูกดึงเข้ามาตั้งแต่ขั้นตอน extract แล้ว
    #           (เพราะไม่ตรงเดือนไหนเลย) จึงไม่ควรเจอ null ในจุดนี้ แต่ใส่ errors="coerce"
    #           ไว้เผื่อความปลอดภัย ถ้าเจอจะได้ไม่ error ทั้งโปรแกรม
    # -------------------------------------------------------------------
    df["date_added"] = pd.to_datetime(
        df["date_added"].str.strip(), errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # -------------------------------------------------------------------
    # ขั้นที่ 4: แยก duration เป็น 2 คอลัมน์ ตามชนิดของเรื่อง (Movie / TV Show)
    # -------------------------------------------------------------------
    # ดึงเฉพาะตัวเลขออกจาก string เช่น "90 min" -> 90.0 , "2 Seasons" -> 2.0
    duration_number = df["duration"].str.extract(r"(\d+)").astype(float)[0]
    is_movie = df["type"] == "Movie"

    # .where(condition) -> เก็บค่าไว้เฉพาะแถวที่เงื่อนไขเป็นจริง แถวอื่นกลายเป็น NaN
    df["duration_minutes"] = duration_number.where(is_movie)
    df["duration_seasons"] = duration_number.where(~is_movie)

    # -------------------------------------------------------------------
    # ขั้นที่ 5: สร้างตาราง genre แยกต่างหาก (normalize จาก listed_in)
    # -------------------------------------------------------------------
    genre_df = df[["show_id", "listed_in"]].copy()
    # แยก string ด้วย comma ให้กลายเป็น list ก่อน เช่น "Sci-Fi, Horror" -> ["Sci-Fi", " Horror"]
    genre_df["genre"] = genre_df["listed_in"].str.split(",")
    # "ระเบิด" 1 แถวที่มี list ให้กลายเป็นหลายแถว โดย show_id เดิมจะถูกคัดลอกติดไปทุกแถว
    genre_df = genre_df.explode("genre")
    # ตัดช่องว่างหน้า/หลังที่ติดมาจากตอน split ด้วย comma (เช่น " Horror" -> "Horror")
    genre_df["genre"] = genre_df["genre"].str.strip()
    genre_df = genre_df[["show_id", "genre"]]

    # -------------------------------------------------------------------
    # ขั้นที่ 6: จัดคอลัมน์สุดท้ายของไฟล์หลัก
    # ตัด listed_in (แยกไปตาราง genre แล้ว), description (ไม่จำเป็นต่อการวิเคราะห์),
    # และ duration ดิบ (แยกเป็น duration_minutes / duration_seasons แล้ว) ทิ้งไป
    # -------------------------------------------------------------------
    main_df = df.drop(columns=["listed_in", "description", "duration"])

    # -------------------------------------------------------------------
    # เช็คว่าโฟลเดอร์ปลายทางทั้งสองมีอยู่จริงหรือไม่ — ไม่สร้างให้อัตโนมัติ
    # ตั้งใจให้ error ทันทีถ้ายังไม่มีโฟลเดอร์ เพื่อบังคับให้ผู้ใช้สร้างโครงสร้างโฟลเดอร์เองก่อนรันเสมอ
    # -------------------------------------------------------------------
    if not transformed_dir.is_dir():
        raise FileNotFoundError(
            f"ไม่พบโฟลเดอร์ปลายทาง: {transformed_dir} — กรุณาสร้างโฟลเดอร์นี้ก่อนรัน script"
        )
    if not genre_dir.is_dir():
        raise FileNotFoundError(
            f"ไม่พบโฟลเดอร์ปลายทาง: {genre_dir} — กรุณาสร้างโฟลเดอร์นี้ก่อนรัน script"
        )

    # เขียนไฟล์ผลลัพธ์ทั้งสอง (เขียนทับเสมอ = idempotent)
    main_path = transformed_dir / file_name
    genre_path = genre_dir / file_name

    main_df.to_csv(main_path, index=False)
    genre_df.to_csv(genre_path, index=False)

    print(
        f"[transform] เดือน {target_year:04d}-{target_month:02d}: "
        f"หลัก {len(main_df)} แถว -> {main_path} | "
        f"genre {len(genre_df)} แถว -> {genre_path}"
    )

    return main_path, genre_path


# ---------------------------------------------------------------------------
# ส่วนทดสอบแบบ standalone (รันตรงๆ ด้วย python transform_csv.py โดยไม่ผ่าน Airflow)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_logical_date = datetime(2021, 9, 1)
    transform_monthly_data(logical_date=test_logical_date)
    