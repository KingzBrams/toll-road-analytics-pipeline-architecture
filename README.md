# Toll Road Corporate Learning ELT Pipeline

> A production-grade, end-to-end ELT data pipeline simulating corporate training analytics for a toll road company. Built with modern data stack architecture: **Apache Airflow**, **dbt**, **PostgreSQL**, and a custom **FastAPI** mock source featuring complex dirty data injection.


## 🛠️ How to Start This Project (Step-by-Step Guide)



### Step 1: Clone Repository & Setup Environment

Clone repository ini ke lokal Anda:

```bash
git clone https://github.com/username/toll-road-analytics-etl-pipeline.git
cd toll-road-analytics-etl-pipeline
```
Buat file `.env` di root direktori *project* Anda, lalu salin konfigurasi berikut:
```env
# Airflow Config
AIRFLOW_UID=5000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin123

# PostgreSQL Data Warehouse Config
POSTGRES_DW_USER=de_admin
POSTGRES_DW_PASSWORD=secure_postgres_password_2026
POSTGRES_DW_DB=toll_learning_dw
POSTGRES_DW_PORT=5433
```

### Step 2: Start Core Infrastructure (Airflow, PostgreSQL DW, & Redis)

Jalankan perintah Docker Compose untuk menghidupkan seluruh infrastruktur utama secara otomatis di dalam *custom network* privat (`toll_dw_network`):

```bash
docker-compose build --no-cache
docker-compose up -d --force-recreate
```

Setelah perintah selesai, **Mock API** akan aktif di port `8000`. Anda dapat mengakses dokumentasi interaktif Swagger UI di browser melalui:
**`http://localhost:8000/docs`**

### Step 3: Access Apache Airflow UI

Buka browser Anda dan akses Airflow Webserver di:
**`http://localhost:8080`**

* **Username**
* **Password**

Di dalam Airflow UI, Anda dapat melihat DAG pipeline yang siap di-trigger untuk menarik data dari Mock API ke *staging* PostgreSQL.


### Step 4: Access PostgreSQL Data Warehouse

Akses PostgreSQL Data Warehouse menggunakan SQL client pilihan Anda (seperti DBeaver, pgAdmin, atau database extension):

* **Host**
* **Port**
* **Username**
* **Password**
* **Database**

Pada database `toll_learning_dw`, Anda dapat melihat tabel-tabel *staging* dan *mart* analitik yang ada di dalamnya.