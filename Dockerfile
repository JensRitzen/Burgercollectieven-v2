# Gebruik een lichte Python image
FROM python:3.12-slim

# Zet werkmap in de container
WORKDIR /app

# Kopieer je code naar de container
COPY . .

# Maak map voor database
RUN mkdir -p /app/data

# Installeer eventuele packages
RUN pip install --no-cache-dir -r requirements.txt

# Start het Python-script
CMD ["python", "-u", "main.py"]

