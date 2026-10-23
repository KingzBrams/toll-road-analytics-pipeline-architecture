from datetime import datetime, timedelta
import random
from typing import List, Optional
from faker import Faker
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

app = FastAPI(
    title="Enterprise Corporate Learning Mock API",
    version="2.0.0",
    description="Advanced Mock API dengan 4 Master Tables & Complex Dirty Data untuk Portofolio Data Engineering.",
)

fake = Faker("id_ID")

# In-Memory Database Storage
DB_EMPLOYEES = []
DB_PROVIDERS = []
DB_COMPETENCIES = []
DB_UNITS = []
DB_ACTIVITIES = []

# Reference Data
UNITS_LIST = [
    "Divisi Operasi Tol",
    "Divisi Pemeliharaan",
    "Divisi Human Capital",
    "Divisi Keuangan & Akuntansi",
    "Divisi Teknologi Informasi",
    "Divisi Legal & Umum",
]
JABATAN_LIST = [
    "Staff Toll Collection",
    "Supervisor Pemeliharaan",
    "Manager Area",
    "Specialist IT Infrastructure",
    "Officer HRD",
    "Senior Auditor",
]
ACADEMIES = [
    "Toll Operation Academy",
    "Leadership Academy",
    "Digital & IT Academy",
    "Maintenance & Engineering Academy",
]
SUB_ACADEMIES = {
    "Toll Operation Academy": [
        "Transaction Management",
        "Traffic Control & Safety",
    ],
    "Leadership Academy": ["Supervisory Skills", "Strategic Management"],
    "Digital & IT Academy": ["Data Engineering", "Cloud Infrastructure"],
    "Maintenance & Engineering Academy": [
        "Pavement Maintenance",
        "Bridge Inspection",
    ],
}
PROVIDERS_LIST = [
    "PT Lembaga Manajemen Indonesia",
    "Garuda Training Center",
    "Internal Corporate Trainer",
    "Prasetya Mulya Executive Learning",
    "AWS Indonesia Training",
]
LOCATIONS = [
    "Head Office Jakarta",
    "Auditorium Gedung Tol Lt. 3",
    "Online Zoom Meeting",
    "Hotel Aston Priority Simatupang",
    "Training Center Cikarang",
]


# ==========================================
# HELPER: ADVANCED DIRTY DATA INJECTION
# ==========================================
def inject_complex_dirty_data(record: dict, probability: float = 0.30) -> dict:
    """Menyisipkan anomali data kotor tingkat lanjut secara acak

    untuk menguji ketahanan pipeline ETL, data cleaning, dan dbt testing.
    """
    if random.random() > probability:
        return record

    dirty_scenario = random.choice(
        [
            "whitespace_mess",
            "case_inconsistency",
            "null_mandatory",
            "orphan_foreign_key",
            "numeric_contamination",
            "negative_budget",
        ]
    )

    if dirty_scenario == "whitespace_mess":
        if isinstance(record.get("nama"), str):
            record["nama"] = f"   {record['nama']}  "
        if isinstance(record.get("unit_kerja"), str):
            record["unit_kerja"] = f"{record['unit_kerja']} \t"

    elif dirty_scenario == "case_inconsistency":
        if isinstance(record.get("category"), str):
            record["category"] = record["category"].upper()
        if isinstance(record.get("jenis_pembelajaran"), str):
            record["jenis_pembelajaran"] = record["jenis_pembelajaran"].lower()

    elif dirty_scenario == "null_mandatory":
        target = random.choice(
            ["kode_komp", "npp", "jam_mulai", "lokasi_kegiatan"]
        )
        record[target] = None

    elif dirty_scenario == "orphan_foreign_key":
        # Menyengaja NPP atau Kode Komp tidak ada di Master Table (Referential Drift)
        if random.random() > 0.5:
            record["npp"] = "EMP-GHOST-999"
        else:
            record["kode_komp"] = "KOMP-UNKNOWN-000"

    elif dirty_scenario == "numeric_contamination":
        # Anggaran tercemar string mata uang atau format teks bebas
        if random.random() > 0.5:
            record["total_anggaran"] = "Rp 7.500.000 (Estimasi)"
        else:
            record["biaya_rab_ldd"] = "Free / Internal"

    elif dirty_scenario == "negative_budget":
        record["budget_realisasi_rab_led"] = -1500000

    return record


# ==========================================
# 1. MASTER EMPLOYEES
# ==========================================
@app.post("/api/v1/masters/employees/generate")
def generate_employees(
    total: int = Query(30, ge=1, le=1000, description="Jumlah data karyawan")
):
    global DB_EMPLOYEES
    DB_EMPLOYEES = []
    for _ in range(total):
        emp = {
            "npp": f"EMP{random.randint(10000, 99999)}",
            "nama": fake.name(),
            "level": random.choice(
                ["Staff", "Supervisor", "Assistant Manager", "Manager"]
            ),
            "grade": f"Class {random.randint(4, 9)}",
            "jabatan": random.choice(JABATAN_LIST),
            "unit_kerja": random.choice(UNITS_LIST),
            "gender": random.choice(["Laki-laki", "Perempuan"]),
            "updated_at": datetime.now().isoformat(),
        }
        DB_EMPLOYEES.append(emp)
    return {
        "status": "success",
        "message": f"Generated {len(DB_EMPLOYEES)} employees.",
    }


@app.get("/api/v1/masters/employees")
def get_employees():
    return {"status": "success", "count": len(DB_EMPLOYEES), "data": DB_EMPLOYEES}


# ==========================================
# 2. MASTER PROVIDERS
# ==========================================
@app.post("/api/v1/masters/providers/generate")
def generate_providers(
    total: int = Query(10, ge=1, le=100, description="Jumlah data provider")
):
    global DB_PROVIDERS
    DB_PROVIDERS = []
    for i in range(total):
        prov = {
            "provider_id": f"PROV{i+1:03d}",
            "learning_provider": (
                PROVIDERS_LIST[i]
                if i < len(PROVIDERS_LIST)
                else fake.company()
            ),
            "lokasi_kegiatan": random.choice(LOCATIONS),
            "updated_at": datetime.now().isoformat(),
        }
        DB_PROVIDERS.append(prov)
    return {
        "status": "success",
        "message": f"Generated {len(DB_PROVIDERS)} providers.",
    }


@app.get("/api/v1/masters/providers")
def get_providers():
    return {"status": "success", "count": len(DB_PROVIDERS), "data": DB_PROVIDERS}


# ==========================================
# 3. MASTER COMPETENCIES (NEW)
# ==========================================
@app.post("/api/v1/masters/competencies/generate")
def generate_competencies(
    total: int = Query(
        20, ge=1, le=200, description="Jumlah katalog kompetensi"
    )
):
    global DB_COMPETENCIES
    DB_COMPETENCIES = []
    for i in range(total):
        acad = random.choice(ACADEMIES)
        sub_acad = random.choice(SUB_ACADEMIES[acad])
        comp = {
            "kode_komp": f"KOMP-{i+100}",
            "nama_kompetensi": f"Kompetensi Lanjutan {sub_acad}",
            "learning_academy": acad,
            "sub_learning_academy": sub_acad,
            "standard_point": random.choice([25, 50, 75, 100]),
            "updated_at": datetime.now().isoformat(),
        }
        DB_COMPETENCIES.append(comp)
    return {
        "status": "success",
        "message": f"Generated {len(DB_COMPETENCIES)} competencies.",
    }


@app.get("/api/v1/masters/competencies")
def get_competencies():
    return {
        "status": "success",
        "count": len(DB_COMPETENCIES),
        "data": DB_COMPETENCIES,
    }


# ==========================================
# 4. MASTER UNITS / COST CENTERS (NEW)
# ==========================================
@app.post("/api/v1/masters/units/generate")
def generate_units():
    global DB_UNITS
    DB_UNITS = []
    for i, unit_name in enumerate(UNITS_LIST):
        unit = {
            "unit_code": f"UNIT-TOLL-{i+1:02d}",
            "unit_kerja": unit_name,
            "annual_budget_plafond": random.randint(50000000, 250000000),
            "updated_at": datetime.now().isoformat(),
        }
        DB_UNITS.append(unit)
    return {"status": "success", "message": f"Generated {len(DB_UNITS)} units."}


@app.get("/api/v1/masters/units")
def get_units():
    return {"status": "success", "count": len(DB_UNITS), "data": DB_UNITS}


# ==========================================
# 5. FACT TRAINING ACTIVITIES (WITH LATE-ARRIVING DATA & INCREMENTAL)
# ==========================================
@app.post("/api/v1/facts/training-activities/generate")
def generate_training_activities(
    total: int = Query(
        5000, ge=1, le=100000, description="Jumlah data transaksi pelatihan"
    ),
    insert_from: Optional[str] = Query(
        None, description="Batas awal tanggal insert_at (format: YYYY-MM-DD)"
    ),
    insert_to: Optional[str] = Query(
        None, description="Batas akhir tanggal insert_at (format: YYYY-MM-DD)"
    ),
):
    global DB_ACTIVITIES

    # Auto-generate master records if empty
    if not DB_EMPLOYEES:
        generate_employees(30)
    if not DB_PROVIDERS:
        generate_providers(10)
    if not DB_COMPETENCIES:
        generate_competencies(20)
    if not DB_UNITS:
        generate_units()

    DB_ACTIVITIES = []

    # Parse custom date range or fallback to standard 120-day window
    try:
        range_start = (
            datetime.strptime(insert_from, "%Y-%m-%d")
            if insert_from
            else datetime.now() - timedelta(days=120)
        )
        range_end = (
            datetime.strptime(insert_to, "%Y-%m-%d") + timedelta(days=1)
            if insert_to
            else datetime.now()
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Format tanggal tidak valid, gunakan YYYY-MM-DD.",
        )

    if range_start >= range_end:
        raise HTTPException(
            status_code=400,
            detail="insert_from harus lebih kecil dari insert_to.",
        )

    total_seconds = max(1, int((range_end - range_start).total_seconds()))

    for i in range(total):
        emp = random.choice(DB_EMPLOYEES)
        prov = random.choice(DB_PROVIDERS)
        comp = random.choice(DB_COMPETENCIES)

        # Generate insert timestamp strictly within the selected range
        random_seconds = random.randint(0, total_seconds)
        insert_timestamp = range_start + timedelta(seconds=random_seconds)
        event_date = insert_timestamp - timedelta(days=random.randint(0, 3))

        biaya_rab = random.choice([1000000, 2500000, 5000000, 10000000])
        realisasi_rab = int(biaya_rab * random.uniform(0.9, 1.05))

        record = {
            "activity_id": f"ACT-{i+1:06d}",
            "nama_kegiatan": f"Pelatihan {comp['sub_learning_academy']} Batch {random.randint(1, 4)}",
            "jenis_pembelajaran": random.choice(["Training", "Learning Events"]),
            "category": random.choice(["Inhouse", "Public"]),
            "type": random.choice(["Online", "Offline", "Hybrid"]),
            "type_event": random.choice(
                ["Mandatory", "Development", "Certification"]
            ),
            "learning_academy": comp["learning_academy"],
            "sub_learning_academy": comp["sub_learning_academy"],
            "kode_komp": comp["kode_komp"],
            "npp": emp["npp"],
            "nama": emp["nama"],
            "level": emp["level"],
            "grade": emp["grade"],
            "jabatan": emp["jabatan"],
            "unit_kerja": emp["unit_kerja"],
            "gender": emp["gender"],
            "status_kegiatan": random.choice(
                ["Peserta", "Narasumber", "MC&Moderator"]
            ),
            "type_narasumber": random.choice(["Internal", "External", "N/A"]),
            "type_mc": random.choice(["Internal", "External", "N/A"]),
            "point": comp["standard_point"],
            "total_poin": comp["standard_point"],
            "tanggal_event": event_date.strftime("%Y-%m-%d"),
            "jam_mulai": "08:00:00",
            "jam_selesai": "16:00:00",
            "kat_sertifikasi": random.choice(
                [
                    "Sertifikat Kelulusan",
                    "Certificate of Attendance",
                    "Non-Sertifikasi",
                ]
            ),
            "inisiator_unit": random.choice(UNITS_LIST),
            "learning_provider": prov["learning_provider"],
            "lokasi_kegiatan": prov["lokasi_kegiatan"],
            "biaya_rab_ldd": biaya_rab,
            "budget_realisasi_rab_led": realisasi_rab,
            "biaya_penginapan": random.choice([0, 500000, 1500000]),
            "biaya_transportasi": random.choice([100000, 300000, 750000]),
            "biaya_konsumsi": 150000,
            "biaya_training_kit": 50000,
            "biaya_fasilitas_pendukung": 0,
            "total_anggaran": realisasi_rab + 500000,
            "purchase_number": f"PO/TOLL/{random.randint(10000, 99999)}/{event_date.strftime('%Y')}",
            "purchase_date": (
                event_date - timedelta(days=random.randint(3, 10))
            ).strftime("%Y-%m-%d"),
            "npp_pic": f"EMP{random.randint(10000, 99999)}",
            "nama_pic": fake.name(),
            "insert_at": insert_timestamp.isoformat(),  # Kolom kunci untuk incremental & late-arriving check
        }

        # Injeksikan data kotor yang kompleks
        record = inject_complex_dirty_data(record, probability=0.25)
        DB_ACTIVITIES.append(record)

    return {
        "status": "success",
        "message": f"Generated {len(DB_ACTIVITIES)} training activities (with late-arriving & complex anomalies).",
    }


@app.get(
    "/api/v1/facts/training-activities",
    summary="Fetch Training Activities",
)
def get_training_activities(
    limit: int = Query(1000, ge=1, le=50000),
    offset: int = Query(0, ge=0),
):
    paginated_data = DB_ACTIVITIES[offset : offset + limit]

    return {
        "status": "success",
        "total_matched": len(DB_ACTIVITIES),
        "limit": limit,
        "offset": offset,
        "data": paginated_data,
    }