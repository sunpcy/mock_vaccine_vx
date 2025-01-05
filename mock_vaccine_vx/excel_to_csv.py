import pandas as pd
import os

# ระบุไฟล์ Excel ต้นทาง
excel_file = "mock_vaccine_vx.xlsx"

# ชื่อโฟลเดอร์ที่ต้องการบันทึกไฟล์ CSV
output_folder = "data"

# สร้างโฟลเดอร์ถ้ายังไม่มี
os.makedirs(output_folder, exist_ok=True)

# โหลดไฟล์ Excel
excel_data = pd.ExcelFile(excel_file)

# อ่านชื่อชีททั้งหมด
sheet_names = excel_data.sheet_names

# แยกชีทและบันทึกเป็นไฟล์ CSV
for sheet_name in sheet_names:
    # อ่านข้อมูลจากชีท
    df = excel_data.parse(sheet_name, dtype=str)
    
    # ตั้งชื่อไฟล์ CSV และระบุ path ในโฟลเดอร์
    csv_file = os.path.join(output_folder, f"{sheet_name}.csv")
    
    # บันทึกข้อมูลเป็น CSV
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")
    print(f"Exported {sheet_name} to {csv_file}")
