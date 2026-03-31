# Utilise la version de Kestra que tu as actuellement
FROM kestra/kestra:latest-full

# Passer en root pour l'installation
USER root

# Copier le requirements.txt (assure-toi qu'il est dans le même dossier)
COPY requirements.txt /app/requirements.txt

# Installer les dépendances
# On utilise --break-system-packages car les images récentes sont basées sur Debian 12
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN ./kestra plugins install io.kestra.plugin:plugin-googleworkspace:LATEST
RUN apt-get update && apt-get install -y --no-install-recommends docker.io && \
    rm -rf /var/lib/apt/lists/*
# Rebasculer vers l'utilisateur kestra pour la sécurité
USER root