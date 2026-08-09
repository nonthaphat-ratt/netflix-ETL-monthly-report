"""
pipeline_dag.py

หน้าที่: ไฟล์ DAG หลักของโปรเจค netflix ETL — บอก Airflow ว่า pipeline นี้
         ต้องรันถี่แค่ไหน เริ่มนับจากวันไหน และมี task อะไรบ้าง

Pipeline เต็มรูปแบบ: extract -> transform -> load

เวอร์ชันนี้เขียนด้วย TaskFlow API (@dag, @task) แทน DAG()/PythonOperator แบบเดิม
ทำงานเหมือนกันทุกประการ แค่เขียนสั้นและอ่านง่ายกว่า (Airflow แนะนำให้ใช้แบบนี้ในปัจจุบัน)
"""

import sys
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# เพิ่ม root ของโปรเจคเข้าไปใน sys.path
# จำเป็นเพราะ Airflow รันไฟล์ DAG นี้แยกต่างหาก ไม่ได้รู้จักโฟลเดอร์ scripts/ ของเรา
# โดยอัตโนมัติ ต้องบอกให้ Python รู้จักตำแหน่งก่อน ถึงจะ import ได้
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# import ตัวช่วยสร้าง DAG แบบ TaskFlow API
# @dag  -> ใช้แทนการเขียน DAG(...) แบบเดิม ห่อรอบฟังก์ชันที่รวม task ทั้งหมดไว้ข้างใน
# @task -> ใช้แทน PythonOperator แบบเดิม ห่อรอบฟังก์ชัน Python ธรรมดาให้กลายเป็น task
from airflow.sdk import dag, task

# import ฟังก์ชันทั้ง 3 ตัว ที่เขียนไว้แล้วในโฟลเดอร์ scripts/
# (ชื่อไฟล์ในนี้อ้างอิงตามชื่อไฟล์จริงในโปรเจค: extract_csv.py, transform_cleaned.py, load_to_DuckDB.py)
from scripts.extract_csv import extract_monthly_data
from scripts.transform_cleaned import transform_monthly_data
from scripts.load_to_DuckDB import load_monthly_data


# ---------------------------------------------------------------------------
# @dag ครอบฟังก์ชันนี้ไว้ -> ตัวฟังก์ชัน netflix_monthly_etl() คือ "แผนผัง" ของ DAG ทั้งก้อน
# พารามิเตอร์ในวงเล็บของ @dag คือค่าตั้งค่าเดียวกับที่เคยใส่ใน DAG(...) แบบเดิมทุกประการ
# ---------------------------------------------------------------------------
@dag(
    dag_id="netflixcsv_monthly_etl",

    # จุดเริ่มนับตาราง ตั้งตามวันที่ข้อมูลจริงเริ่มมี (เจอจากตอนทำ EDA)
    start_date=datetime(2008, 1, 1),

    # ความถี่ในการรัน: รายเดือน
    schedule="@monthly",

    # ปิดการรันย้อนหลังไว้ก่อนตอนพัฒนา/ทดสอบ
    # พอมั่นใจว่า pipeline ทำงานถูกต้องแล้ว ค่อยเปลี่ยนเป็น True เพื่อ backfill ย้อนหลังทีหลัง
    catchup=False,

    description="ดึง-แปลง-โหลดข้อมูล Netflix titles รายเดือน จำลองจากไฟล์ CSV แบบ static",
    tags=["netflix", "etl"],
)
def netflix_monthly_etl():
    """แผนผัง DAG: extract -> transform -> load ตามลำดับ"""

    # -----------------------------------------------------------------------
    # แต่ละ @task คือ 1 task ใน DAG
    # เหมือนเดิมกับตอนใช้ PythonOperator: Airflow จะดูชื่อพารามิเตอร์ในฟังก์ชัน
    # แล้วจับคู่กับ context ของรอบนั้นให้เองอัตโนมัติ (ในที่นี้คือ logical_date)
    # -----------------------------------------------------------------------

    @task
    def extract(logical_date=None):
        extract_monthly_data(logical_date=logical_date)

    @task
    def transform(logical_date=None):
        transform_monthly_data(logical_date=logical_date)

    @task
    def load(logical_date=None):
        load_monthly_data(logical_date=logical_date)

    # -----------------------------------------------------------------------
    # กำหนดลำดับการรัน: extract ต้องเสร็จก่อน transform ถึงจะเริ่มได้
    # และ transform ต้องเสร็จก่อน load ถึงจะเริ่มได้ (รันเรียงกันตามลำดับ ไม่ใช่พร้อมกัน)
    # การเรียก extract(), transform(), load() คือการสร้าง task instance ของแต่ละตัว
    # ส่วน >> คือการบอกลำดับก่อน-หลัง อ่านว่า "แล้วต่อด้วย"
    # -----------------------------------------------------------------------
    extract() >> transform() >> load()


# เรียกฟังก์ชันที่มี @dag ครอบไว้ 1 ครั้ง เพื่อให้ Airflow "จดทะเบียน" DAG นี้เข้าระบบจริงๆ
# (ถ้าลืมเรียกบรรทัดนี้ Airflow จะไม่เห็น DAG เลย แม้โค้ดข้างบนจะถูกต้องหมดก็ตาม)
netflix_monthly_etl()