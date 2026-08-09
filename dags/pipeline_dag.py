import sys
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# เพิ่ม root ของโปรเจคเข้าไปใน sys.path
# จำเป็นเพราะ Airflow รันไฟล์ DAG นี้แยกต่างหาก ไม่ได้รู้จักโฟลเดอร์ scripts/ ของเรา
# โดยอัตโนมัติ ต้องบอกให้ Python รู้จักตำแหน่งก่อน ถึงจะ import ได้
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# import ฟังก์ชันที่เขียนไว้แล้วใน scripts/extract_csv.py
from scripts.extract_csv import extract_monthly_data


# ---------------------------------------------------------------------------
# ประกาศ DAG object — เป็น "ภาชนะ" ที่ครอบทุก task ไว้ข้างใน
# ---------------------------------------------------------------------------
with DAG(
    dag_id="netflixcsv_monthly_etl",

    # จุดเริ่มนับตาราง ตั้งตามวันที่ข้อมูลจริงเริ่มมี (เจอจากตอนทำ EDA)
    start_date=datetime(2008, 1, 1),

    # ความถี่ในการรัน: รายเดือน
    # หมายเหตุ: ถ้า Airflow เวอร์ชันเก่ากว่า 2.4 ต้องใช้ชื่อพารามิเตอร์ schedule_interval แทน
    schedule="@monthly",

    # ปิดการรันย้อนหลังไว้ก่อนตอนพัฒนา/ทดสอบ (ตามที่ตกลงกันไว้)
    # พอมั่นใจว่า pipeline ทำงานถูกต้องแล้ว ค่อยเปลี่ยนเป็น True เพื่อ backfill ย้อนหลังทีหลัง
    catchup=False,

    description="ดึงข้อมูล Netflix titles รายเดือน จำลองจากไฟล์ CSV แบบ static",
    tags=["netflix", "etl"],
) as dag:

    # -----------------------------------------------------------------------
    # Task: extract_monthly_data
    #
    # PythonOperator จะเป็นตัวกลางเรียกฟังก์ชัน extract_monthly_data ให้เรา
    # โดย Airflow จะดูชื่อพารามิเตอร์ในฟังก์ชัน แล้วจับคู่กับ context ของรอบนั้นให้เอง
    # ในที่นี้ฟังก์ชันมีพารามิเตอร์ชื่อ "logical_date" ตรงกับ context พอดี
    # Airflow จึงส่งค่าที่ถูกต้องของรอบนั้นเข้ามาให้อัตโนมัติ ไม่ต้องเขียนอะไรเพิ่ม
    #
    # ส่วน raw_csv_path และ output_dir ไม่ใช่ชื่อที่ Airflow รู้จัก
    # จึงใช้ค่า default ที่กำหนดไว้แล้วในไฟล์ extract_csv.py โดยอัตโนมัติ
    # -----------------------------------------------------------------------
    extract_task = PythonOperator(
        task_id="extract_monthly_data",
        python_callable=extract_monthly_data,
    )

    # ตอนนี้มี task เดียว จึงยังไม่ต้องกำหนดลำดับด้วย >>
    # พอมี transform_task, load_task เพิ่มทีหลัง จะมาต่อแบบนี้:
    #   extract_task >> transform_task >> load_task
    extract_task