FROM odoo:17.0

USER root
COPY requirements.txt /tmp/foggy-odoo-bridge-requirements.txt
RUN pip install --no-cache-dir -r /tmp/foggy-odoo-bridge-requirements.txt \
    && rm /tmp/foggy-odoo-bridge-requirements.txt
USER odoo
