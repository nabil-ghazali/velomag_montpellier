FROM python:3.12-slim

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
# COPY le model de prediction


# Changez EXPOSE 8000 en 80
EXPOSE 80

# Changez le port dans la commande de démarrage
CMD ["uvicorn", "backend.api.api:app", "--host", "0.0.0.0", "--port", "80"]