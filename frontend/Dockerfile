FROM python:3.10-slim

WORKDIR /app

# --- CORRECTION ICI ---
# On copie le fichier spécifiquement depuis le dossier frontend
COPY frontend/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# On copie le code du frontend
COPY frontend/ ./frontend/

EXPOSE 8501

CMD ["streamlit", "run", "frontend/app.py", "--server.port=8501", "--server.address=0.0.0.0"]