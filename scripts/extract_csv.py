"""
extract.py

หน้าที่: ดึงข้อมูล Netflix titles เฉพาะ "เดือน" ที่ Airflow กำหนดมาให้ ออกจากไฟล์ CSV ต้นฉบับ
         (จำลองการ extract ข้อมูลแบบรายเดือน จากไฟล์ static ก้อนเดียว)

หลักการที่ script นี้ยึดตาม (สรุปจากที่เราคุยกันมา):
1. Airflow-native  -> ไม่ hardcode เดือนไว้ในโค้ด แต่รับค่าเวลาเข้ามาทางพารามิเตอร์ logical_date
2. Idempotent      -> รันซ้ำกี่ครั้งสำหรับเดือนเดียวกัน ต้องได้ผลลัพธ์เหมือนเดิมเป๊ะ (เขียนทับไฟล์เดิม)
3. แยกงานตามมิติเวลา -> แถวที่ date_added เป็น null ไม่มีมิติเวลา จึงไม่ถูกจัดการในนี้
                        (จะทำเป็น one-time data quality script แยกต่างหากทีหลัง)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# ค่าคงที่ของพาธไฟล์ (default) — โครงสร้างอิงตามโฟลเดอร์ data/ ในโปรเจค
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CSV_PATH = PROJECT_ROOT / "data" / "netflix_titles.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "extracted"


def extract_monthly_data(
    logical_date: datetime,
    raw_csv_path: Path = RAW_CSV_PATH,
    output_dir: Path = OUTPUT_DIR,
) -> Path:
    """
    ดึงแถวข้อมูลที่ date_added อยู่ในเดือนเดียวกับ logical_date ออกมาเป็นไฟล์แยก

    Parameters
    ----------
    logical_date : datetime
        ค่าที่ Airflow ส่งเข้ามาในแต่ละรอบ ใช้แค่ปีและเดือน (ไม่สนใจวันที่)
        ฟังก์ชันนี้ไม่ได้ผูกกับ Airflow โดยตรง เพื่อให้ทดสอบแบบ standalone ได้ง่าย
        (ตัว DAG จะเป็นคนส่ง logical_date เข้ามาให้ตอนเรียกใช้จริง)
    raw_csv_path : Path
        พาธของไฟล์ CSV ต้นฉบับ (ไฟล์เดียว ไม่เปลี่ยนแปลงทุกรอบ)
    output_dir : Path
        โฟลเดอร์ปลายทางที่จะเก็บไฟล์ผลลัพธ์รายเดือน

    Returns
    -------
    Path
        พาธของไฟล์ผลลัพธ์ที่เขียนออกมาในรอบนี้
    """
    target_year = logical_date.year
    target_month = logical_date.month

    # โหลดไฟล์ CSV ต้นฉบับทั้งไฟล์ (ไฟล์เดียวกันทุกรอบ)
    df = pd.read_csv(raw_csv_path)

    # แปลงคอลัมน์ date_added จาก string (เช่น "September 25, 2021") ให้เป็น datetime
    # errors="coerce" -> ถ้าแปลงไม่ได้หรือเป็นค่าว่าง จะได้ NaT (Not a Time) แทนที่จะทำให้โปรแกรม error
    # .str.strip() -> ตัดช่องว่างหน้า/หลัง ที่พบได้ในข้อมูลบางแถว
    parsed_dates = pd.to_datetime(df["date_added"].str.strip(), errors="coerce")

    # กรองเฉพาะแถวที่ปี และเดือน ของ date_added ตรงกับ logical_date ที่ Airflow ส่งมา
    # หมายเหตุ: แถวที่ date_added เป็น null (กลายเป็น NaT) จะไม่ตรงเงื่อนไขนี้เลย
    #           -> ไม่ถูกดึงเข้ารอบไหนทั้งนั้น เป็นไปตามที่ออกแบบไว้ (จัดการแยกต่างหาก)
    month_mask = (parsed_dates.dt.year == target_year) & (parsed_dates.dt.month == target_month)
    monthly_df = df[month_mask]

    # เช็คว่าโฟลเดอร์ปลายทางมีอยู่จริงหรือไม่ — ไม่สร้างให้อัตโนมัติ
    # ตั้งใจให้ error ทันทีถ้ายังไม่มีโฟลเดอร์ เพื่อบังคับให้ผู้ใช้สร้างโครงสร้างโฟลเดอร์เองก่อนรันเสมอ
    if not output_dir.is_dir():
        raise FileNotFoundError(
            f"ไม่พบโฟลเดอร์ปลายทาง: {output_dir} — กรุณาสร้างโฟลเดอร์นี้ก่อนรัน script"
        )

    # ตั้งชื่อไฟล์ตามปี-เดือนของรอบนี้ เช่น 2008-01.csv
    output_filename = f"{target_year:04d}-{target_month:02d}.csv"
    output_path = output_dir / output_filename

    # เขียนทับไฟล์เดิมเสมอ (idempotent) — รันซ้ำกี่ครั้งก็ได้ไฟล์ผลลัพธ์เดิม ไม่มีไฟล์ (1), (2) ค้าง
    monthly_df.to_csv(output_path, index=False)

    # log สรุปผลของรอบนี้ ช่วยให้เช็คย้อนหลังง่ายเวลาดูใน Airflow logs
    print(f"[extract] เดือน {target_year:04d}-{target_month:02d}: ดึงได้ {len(monthly_df)} แถว -> {output_path}")

    return output_path


# ---------------------------------------------------------------------------
# ส่วนทดสอบแบบ standalone (รันตรงๆ ด้วย python extract.py โดยไม่ผ่าน Airflow)
# ใช้เช็คว่า logic ทำงานถูกต้อง ก่อนเอาไปต่อกับ Airflow DAG จริง
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_logical_date = datetime(2021, 9, 1)
    extract_monthly_data(logical_date=test_logical_date)