"""
tests/test_dag_integrity.py

เช็คว่าไฟล์ dags/pipeline_dag.py "import ได้โดยไม่ error"
พูดง่ายๆ คือเช็คว่า Airflow จะ parse ไฟล์นี้แล้วพังหรือเปล่า (เช่น syntax ผิด, import ผิด,
decorator ใช้ผิดวิธี) — ไม่ได้ทดสอบว่า logic การ extract/transform/load ถูกต้อง 100%
(อันนั้นควรมี unit test แยกต่างหากทีหลัง ถ้าอยากให้ช่วยเขียนบอกได้เลย)
"""

import importlib.util
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAG_FILE = PROJECT_ROOT / "dags" / "pipeline_dag.py"


def test_dag_file_imports_without_error():
    # โหลดไฟล์ pipeline_dag.py ตรงๆ เหมือนที่ Airflow จะทำตอน parse DAG
    # ถ้าไฟล์มี error ระหว่าง import (syntax ผิด, import ผิด, decorator ใช้ผิด)
    # บรรทัด exec_module จะ raise exception ทันที ทำให้ test นี้ fail โดยอัตโนมัติ
    spec = importlib.util.spec_from_file_location("pipeline_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # เช็คว่ามีฟังก์ชัน DAG หลักชื่อ netflix_monthly_etl อยู่จริงในไฟล์ (กันเผลอลบทิ้ง)
    assert hasattr(module, "netflix_monthly_etl")