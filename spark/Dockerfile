# Utilise la version de Kestra que tu as actuellement
FROM apache/spark:3.5.3

# Passer en root pour l'installation
USER root

# Installer les dépendances
# On utilise --break-system-packages car les images récentes sont basées sur Debian 12
RUN pip install cryptography

# Rebasculer vers l'utilisateur kestra pour la sécurité
USER 185