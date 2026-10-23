# ระบบรวบรวมข้อมูล Notebook มือสอง (5,000–30,000 บาท)

โปรเจกต์ข้อสอบปลายภาค วิชา 02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์
มหาวิทยาลัยเกษตรศาสตร์ วิทยาเขตกำแพงแสน

- ผู้จัดทำ: ทวีศักดิ์ นาคทับ รหัสนิสิต 6621600852 หมู่ 700

## หัวข้อและขอบเขต

- **หมวด:** Notebook มือสอง
- **ขอบเขต:** เปรียบเทียบราคาและสเปก Notebook มือสองจากหลายแหล่งซื้อขาย ในช่วงราคา 5,000–30,000 บาท
- **เป้าหมาย:** ดึงประกาศจากหลายเว็บ รวมเป็นชุดข้อมูลเดียว (schema เดียวกัน) ด้วย Apache Airflow เก็บใน PostgreSQL และแสดงผลบนเว็บ dashboard (Streamlit)

## แหล่งข้อมูล

| แหล่ง | URL | วิธีดึง | สถานะ |
|---|---|---|---|
| Talata | https://talata.co.th/en/categories/electronics/laptops | web scraping (requests + BeautifulSoup) มี 2 หน้า ลูปหยุดเมื่อเจอ 404 | ใช้งาน |
| S Sale Com | https://s-salecom.com/product-category/notebook/ | web scraping (requests + BeautifulSoup) | ใช้งาน |

หมายเหตุ: ไม่ใช้ Selenium ตามข้อกำหนดของโจทย์

### แหล่งที่ตรวจแล้วแต่ไม่ได้ใช้

- **Kaidee:** หน้ารายการเรนเดอร์ด้วย JavaScript ดึงด้วย `requests` ไม่ได้ และโจทย์ห้ามใช้ Selenium
- **OnlineOOPS:** ไม่อนุญาตการเข้าถึงแบบอัตโนมัติ (ตรวจพบจากเครื่องมือที่ใช้ตรวจ จึงไม่ดึงข้อมูล) เปลี่ยนไปใช้ S Sale Com แทน

## การปฏิบัติตาม robots.txt และ PDPA

- ทุก task `extract` เรียก `RobotFileParser` ตรวจ robots.txt ก่อนดึงข้อมูลทุกครั้ง ถ้าไม่อนุญาต task จะล้มทันที
- robots.txt ของ s-salecom.com ไม่ห้ามหน้า `/product-category/notebook/`
- เว้นช่วง 1.5 วินาทีระหว่างคำขอ และตั้ง User-Agent ที่ระบุตัวตน
- **ไม่เก็บข้อมูลส่วนบุคคล** เก็บเฉพาะข้อมูลสินค้า (ชื่อรุ่น ราคา สเปก สภาพ จังหวัด และ URL ประกาศ) ไม่เก็บชื่อ เบอร์โทร หรือที่อยู่ของผู้ขาย

## สถาปัตยกรรม

```
Talata      -> [extract -> transform] \
                                        >-> load (UPSERT) -> PostgreSQL -> Streamlit dashboard
S Sale Com  -> [extract -> transform] /
 (2 TaskGroup รันขนานกัน)
```

- **Airflow DAG:** `notebook_pipeline` (`dags/notebook_pipeline.py`)
  - TaskGroup `talata` และ `ssalecom` รันขนานกัน แต่ละกลุ่มมี extract -> transform
  - task `load` รวมข้อมูลทั้งสองแหล่งและเขียนลงตาราง `listings` ด้วย `INSERT ... ON CONFLICT (source, listing_id) DO UPDATE` (UPSERT) รันซ้ำแล้วจำนวนแถวไม่เพิ่ม
- **PostgreSQL (warehouse):** ตาราง `listings` สร้างอัตโนมัติจาก `sql/init.sql`
- **Dashboard:** Streamlit (`web/app.py`) อ่านข้อมูลจาก PostgreSQL โดยตรง

### Schema (ตาราง `listings`)

| คอลัมน์ | ความหมาย |
|---|---|
| source | แหล่งข้อมูล (`talata`, `ssalecom`) |
| listing_id | รหัสประกาศ (คีย์หลักร่วมกับ source) |
| url | ลิงก์ประกาศ |
| title | ชื่อประกาศ |
| brand | แบรนด์ (แยกจากชื่อ) |
| cpu | ตระกูล CPU (แยกจากชื่อ) |
| ram_gb | RAM (GB) |
| storage_gb | พื้นที่จัดเก็บ (GB) |
| price | ราคา (บาท) |
| province | จังหวัด |
| condition | สภาพสินค้า |
| scraped_at | เวลาที่ดึงข้อมูลล่าสุด |

## วิธีรัน

ต้องติดตั้ง Docker และ Docker Compose

```bash
# 1) สร้างไฟล์ตั้งค่า แล้วแก้รหัสผ่านตามต้องการ
cp .env.example .env

# 2) เริ่มระบบทั้งหมด (ครั้งแรกใช้เวลาสักครู่ เพราะ Airflow ต้องติดตั้งแพ็กเกจและสร้างฐานข้อมูล)
docker compose up -d --build

# 3) ดู log ของ Airflow จนกว่าจะพร้อม
docker compose logs -f airflow
```

| บริการ | ที่อยู่ |
|---|---|
| Airflow | http://localhost:8080 |
| Dashboard | http://localhost:8501 |
| PostgreSQL (warehouse) | localhost:5433 |

### เข้าสู่ระบบ Airflow

username คือ `admin` ดูรหัสผ่านได้จากคำสั่ง

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

หรือตั้งรหัสผ่านใหม่

```bash
docker compose exec airflow airflow users reset-password --username admin --password <รหัสผ่านใหม่>
```

### ใช้งาน

1. เปิด Airflow แล้ว Trigger DAG `notebook_pipeline`
2. รอให้ทุก task เป็นสีเขียว
3. เปิด Dashboard ที่ http://localhost:8501

### ตรวจสอบ UPSERT (รันซ้ำแล้วจำนวนแถวไม่เพิ่ม)

```bash
docker compose exec warehouse psql -U warehouse -d notebooks \
  -c "select source, count(*) from listings group by 1"
```

Trigger DAG อีกรอบ แล้วรันคำสั่งนี้ซ้ำ จำนวนแถวต้องเท่าเดิม
(ถ้าแก้ค่า `WH_USER` / `WH_DB` ใน `.env` ให้ใช้ค่าเดียวกันในคำสั่ง)

## ฟีเจอร์ของ Dashboard

- ตารางข้อมูลทั้งหมด
- KPI: จำนวนประกาศ, ราคาเฉลี่ย, ราคาต่ำสุด, RAM มัธยฐาน
- ตัวกรอง: แหล่งข้อมูล, แบรนด์, ช่วงราคา
- กราฟราคาเฉลี่ยตามแบรนด์

## โครงสร้างโปรเจกต์

```
.
├── dags/
│   └── notebook_pipeline.py
├── web/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── sql/
│   └── init.sql
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## ความคืบหน้าตามระดับของโจทย์

- [x] ระดับ 1: ดึงข้อมูล -> DAG (extract -> transform -> load) -> PostgreSQL -> เว็บแสดงตารางและกราฟ
- [ ] ระดับ 2: 2 แหล่งรันขนานด้วย TaskGroup, schema เดียวกัน, UPSERT, KPI และตัวกรอง (ติ๊กเมื่อทดสอบผ่านทั้ง 2 แหล่งแล้ว)
- [ ] ระดับ 3: แหล่งที่ 3, ทนต่อแหล่งที่ล่ม, ตัดรายการซ้ำข้ามแหล่ง, ตรวจคุณภาพข้อมูลและแตกเส้นทาง
- [ ] โบนัส: หน้าสถานะ pipeline หรือ Dynamic Task Mapping

## ปัญหาที่พบและวิธีแก้

| อาการ | สาเหตุ | วิธีแก้ |
|---|---|---|
| task `extract_talata` ล้มด้วย `Connection refused` | เส้นทางเครือข่ายไปบาง IP ของ talata.co.th (CloudFront) timeout เหลือเพียงบาง IP ที่เชื่อมต่อได้ ไม่ใช่บั๊กในโค้ด | ใช้ `requests.Session` + connect timeout สั้น + `Retry` และตั้ง `retries=2` ใน DAG |
| `extract_talata` ล้มด้วย `404` ที่ `?page=3` | หมวดนี้มีแค่ 2 หน้า แต่ลูปไม่ได้จัดการกรณีเกินหน้าสุดท้าย | หยุดลูปเมื่อเจอ 404 |
| Kaidee / OnlineOOPS ดึงไม่ได้ | Kaidee เรนเดอร์ด้วย JavaScript (โจทย์ห้าม Selenium), OnlineOOPS ไม่อนุญาตการเข้าถึงอัตโนมัติ | ใช้ Talata และ S Sale Com แทน |
| เข้า Airflow ไม่ได้ในช่วงแรก | `standalone` ใช้เวลาเริ่มต้นนาน และรหัสผ่านถูกสร้างอัตโนมัติ | รอ log ขึ้น `Login with username` หรือตั้งรหัสผ่านเองด้วยคำสั่งด้านบน |

### ถ้า task extract ล้มเพราะเครือข่าย

ลองรันซ้ำ (DAG ตั้ง retry ไว้แล้ว) ตรวจว่า container ออกเน็ตได้ และหากเครือข่ายที่ใช้บล็อกเว็บต้นทาง ให้สลับเครือข่ายหรือรอสักครู่ ไม่แนะนำให้เลี่ยงการบล็อก

## ข้อจำกัดที่ทราบ

- การแยก CPU / RAM / Storage ใช้ regex จากชื่อประกาศ ประกาศที่ไม่ระบุสเปกในชื่อจะได้ค่าว่าง
- Airflow ติดตั้งแพ็กเกจเพิ่มตอน container เริ่ม (`_PIP_ADDITIONAL_REQUIREMENTS`) ทำให้เริ่มช้า เหมาะสำหรับงานทดสอบ ไม่เหมาะกับ production
- โครงสร้าง HTML ของเว็บต้นทางอาจเปลี่ยน ทำให้ scraper ต้องแก้ไข
- Talata มีประกาศให้ดึงได้เพียง 2 หน้า (หลังกรองเฉพาะสภาพ Used และราคา 5,000–30,000 บาท อาจเหลือน้อยกว่า 80 รายการ) จำนวนข้อมูลรวม 100 รายการขึ้นกับ S Sale Com ด้วย
- ข้อมูลจาก S Sale Com มาจากร้านเดียว ส่วน Talata มีหลายรายการจากร้านเดียวกัน อาจทำให้ข้อมูลเอียงไปทางบางร้าน