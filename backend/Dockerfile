# Base image
FROM python:3.10-slim

# Dossier de travail dans le conteneur
WORKDIR /app

# Copier explicitement le fichier requirements.txt depuis le contexte de build
COPY ./requirements.txt /app/requirements.txt

# Installer les dépendances
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copier tout le contenu du dossier backend dans /app
COPY . /app

# Ajouter le path pour Python
ENV PYTHONPATH="/app"

# Lancer l'API FastAPI
CMD ["uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8000"]

# FROM python:3.10-slim

# # Dossier de travail dans le conteneur
# WORKDIR /app

# # Copier les requirements du backend
# COPY requirements.txt /app/requirements.txt

# # Installer les dépendances
# RUN pip install --no-cache-dir -r requirements.txt

# # Copier tout le dossier backend dans /app
# COPY . /app

# ENV PYTHONPATH="/app"


# # Lancer l'API FastAPI
# CMD ["uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8000"]

# # -----------------------------
# # Dockerfile backend VéloMag
# # -----------------------------
# FROM python:3.10-slim

# # ⚡ Variables pour éviter les fichiers .pyc et le buffering
# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# # 🗂️ Répertoire de travail
# WORKDIR /app/backend

# # 📦 Copier et installer les dépendances
# COPY backend/requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # 📝 Copier tout le code backend
# COPY backend/ .

# # 🔓 Exposer le port
# EXPOSE 8000

# # 🚀 Lancer Uvicorn (FastAPI) sur toutes les interfaces
# CMD ["uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

