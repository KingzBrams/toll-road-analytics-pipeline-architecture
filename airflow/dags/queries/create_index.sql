-- ====================================================================
-- INDEXES FOR STAGING LAYER
-- ====================================================================

-- Staging Employee
CREATE INDEX IF NOT EXISTS idx_stg_employee_updated_at 
    ON staging.stg_employee (updated_at);

-- Staging Provider
CREATE INDEX IF NOT EXISTS idx_stg_provider_updated_at 
    ON staging.stg_provider (updated_at);

-- Staging Competency
CREATE INDEX IF NOT EXISTS idx_stg_competency_updated_at 
    ON staging.stg_competency (updated_at);

-- Staging Unit
CREATE INDEX IF NOT EXISTS idx_stg_unit_updated_at 
    ON staging.stg_unit (updated_at);

-- Staging Activity
CREATE INDEX IF NOT EXISTS idx_stg_activity_insert_at 
    ON staging.stg_activity (insert_at);

-- ====================================================================
-- INDEXES FOR INTERMEDIATE LAYER
-- ====================================================================

-- Intermediate Employee
CREATE INDEX IF NOT EXISTS idx_int_employee_npp 
    ON intermediate.int_employee (npp);
CREATE INDEX IF NOT EXISTS idx_int_employee_unit_kerja 
    ON intermediate.int_employee (unit_kerja);

-- Intermediate Provider
CREATE INDEX IF NOT EXISTS idx_int_provider_id 
    ON intermediate.int_provider (provider_id);

-- Intermediate Competency
CREATE INDEX IF NOT EXISTS idx_int_competency_kode 
    ON intermediate.int_competency (kode_komp);
CREATE INDEX IF NOT EXISTS idx_int_competency_academy 
    ON intermediate.int_competency (learning_academy);

-- Intermediate Unit
CREATE INDEX IF NOT EXISTS idx_int_unit_code 
    ON intermediate.int_unit (unit_code);

-- Intermediate Activity
CREATE INDEX IF NOT EXISTS idx_int_activity_id 
    ON intermediate.int_activity (activity_id);
CREATE INDEX IF NOT EXISTS idx_int_activity_npp 
    ON intermediate.int_activity (npp);
CREATE INDEX IF NOT EXISTS idx_int_activity_kode_komp 
    ON intermediate.int_activity (kode_komp);
CREATE INDEX IF NOT EXISTS idx_int_activity_tanggal_event 
    ON intermediate.int_activity (tanggal_event);
CREATE INDEX IF NOT EXISTS idx_int_activity_insert_at 
    ON intermediate.int_activity (insert_at);
