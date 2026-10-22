FROM python:3.10-slim

WORKDIR /app

# Salin dependencies dari root project
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin seluruh isi folder generator ke dalam container
COPY generator/ ./generator/

EXPOSE 8000

# Jalankan Uvicorn dengan menunjuk ke modul di dalam folder generator
CMD ["uvicorn", "generator.data_generator_api:app", "--host", "0.0.0.0", "--port", "8000"]