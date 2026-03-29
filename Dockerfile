# Odoo 17 + optional AI Chat dependencies
# Build: docker compose build odoo
FROM odoo:17.0

# Install LLM SDKs for built-in AI Chat (optional feature).
# If you do not use AI Chat, you can remove these lines.
USER root
RUN pip install --no-cache-dir openai anthropic
USER odoo
