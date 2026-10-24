-- ====================================================================
-- SCHEMAS INITIALIZATION
-- ====================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- ====================================================================
-- 1. STAGING TABLES (Raw & Permissive Types)
-- ====================================================================

-- Master Employee
CREATE TABLE IF NOT EXISTS staging.stg_employee (
    npp TEXT,
    nama TEXT,
    level TEXT,
    grade TEXT,
    jabatan TEXT,
    unit_kerja TEXT,
    gender TEXT,
    updated_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master Provider
CREATE TABLE IF NOT EXISTS staging.stg_provider (
    provider_id TEXT,
    learning_provider TEXT,
    lokasi_kegiatan TEXT,
    updated_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master Competency
CREATE TABLE IF NOT EXISTS staging.stg_competency (
    kode_komp TEXT,
    nama_kompetensi TEXT,
    learning_academy TEXT,
    sub_learning_academy TEXT,
    standard_point TEXT,
    updated_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master Unit / Cost Center
CREATE TABLE IF NOT EXISTS staging.stg_unit (
    unit_code TEXT,
    unit_kerja TEXT,
    annual_budget_plafond TEXT,
    updated_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Training Activities
CREATE TABLE IF NOT EXISTS staging.stg_activity (
    activity_id TEXT,
    nama_kegiatan TEXT,
    jenis_pembelajaran TEXT,
    category TEXT,
    type TEXT,
    type_event TEXT,
    learning_academy TEXT,
    sub_learning_academy TEXT,
    kode_komp TEXT,
    npp TEXT,
    nama TEXT,
    level TEXT,
    grade TEXT,
    jabatan TEXT,
    unit_kerja TEXT,
    gender TEXT,
    status_kegiatan TEXT,
    type_narasumber TEXT,
    type_mc TEXT,
    point TEXT,
    total_poin TEXT,
    tanggal_event TEXT,
    jam_mulai TEXT,
    jam_selesai TEXT,
    kat_sertifikasi TEXT,
    inisiator_unit TEXT,
    learning_provider TEXT,
    lokasi_kegiatan TEXT,
    biaya_rab_ldd TEXT,
    budget_realisasi_rab_led TEXT,
    biaya_penginapan TEXT,
    biaya_transportasi TEXT,
    biaya_konsumsi TEXT,
    biaya_training_kit TEXT,
    biaya_fasilitas_pendukung TEXT,
    total_anggaran TEXT,
    purchase_number TEXT,
    purchase_date TEXT,
    npp_pic TEXT,
    nama_pic TEXT,
    insert_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- 2. INTERMEDIATE TABLES (Cleaned & Strongly Typed)
-- ====================================================================

-- Master Employee
CREATE TABLE IF NOT EXISTS intermediate.int_employee (
    npp VARCHAR(50),
    nama VARCHAR(255),
    level VARCHAR(100),
    grade VARCHAR(50),
    jabatan VARCHAR(150),
    unit_kerja VARCHAR(150),
    gender VARCHAR(50),
    updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master Provider
CREATE TABLE IF NOT EXISTS intermediate.int_provider (
    provider_id VARCHAR(50),
    learning_provider VARCHAR(255),
    lokasi_kegiatan VARCHAR(255),
    updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master Competency
CREATE TABLE IF NOT EXISTS intermediate.int_competency (
    kode_komp VARCHAR(50),
    nama_kompetensi VARCHAR(255),
    learning_academy VARCHAR(150),
    sub_learning_academy VARCHAR(150),
    standard_point INT,
    updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master Unit
CREATE TABLE IF NOT EXISTS intermediate.int_unit (
    unit_code VARCHAR(50),
    unit_kerja VARCHAR(150),
    annual_budget_plafond NUMERIC(18, 2),
    updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Training Activities
CREATE TABLE IF NOT EXISTS intermediate.int_activity (
    activity_id VARCHAR(50),
    nama_kegiatan VARCHAR(255),
    jenis_pembelajaran VARCHAR(100),
    category VARCHAR(100),
    type VARCHAR(100),
    type_event VARCHAR(100),
    learning_academy VARCHAR(150),
    sub_learning_academy VARCHAR(150),
    kode_komp VARCHAR(50),
    npp VARCHAR(50),
    nama VARCHAR(255),
    level VARCHAR(100),
    grade VARCHAR(50),
    jabatan VARCHAR(150),
    unit_kerja VARCHAR(150),
    gender VARCHAR(50),
    status_kegiatan VARCHAR(100),
    type_narasumber VARCHAR(100),
    type_mc VARCHAR(100),
    point INT,
    total_poin INT,
    tanggal_event DATE,
    jam_mulai TIME,
    jam_selesai TIME,
    kat_sertifikasi VARCHAR(150),
    inisiator_unit VARCHAR(150),
    learning_provider VARCHAR(255),
    lokasi_kegiatan VARCHAR(255),
    biaya_rab_ldd NUMERIC(18, 2),
    budget_realisasi_rab_led NUMERIC(18, 2),
    biaya_penginapan NUMERIC(18, 2),
    biaya_transportasi NUMERIC(18, 2),
    biaya_konsumsi NUMERIC(18, 2),
    biaya_training_kit NUMERIC(18, 2),
    biaya_fasilitas_pendukung NUMERIC(18, 2),
    total_anggaran NUMERIC(18, 2),
    purchase_number VARCHAR(100),
    purchase_date DATE,
    npp_pic VARCHAR(50),
    nama_pic VARCHAR(255),
    insert_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
