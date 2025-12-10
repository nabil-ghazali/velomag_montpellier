FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# --- CORRECTION ICI ---
# On copie le fichier spécifiquement depuis le dossier backend
COPY backend/requirements.txt .

# Installation des dépendances
RUN pip install --no-cache-dir -r requirements.txt

# On copie le code du backend
COPY backend/ ./backend/
# (Optionnel) Si vous avez des dossiers partagés à la racine (ex: database), copiez-les ici :
# COPY database/ ./database/

EXPOSE 8000

# Lancement
CMD ["uvicorn", "backend.api.api:app", "--host", "0.0.0.0", "--port", "8000"]