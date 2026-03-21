#!/bin/bash
# -*- coding: utf-8 -*-
#
# Odoo Module Upgrade Script
# Stops Odoo, upgrades a specified module, then restarts Odoo.
#
# Usage:
#   ./upgrade_module.sh [module_name] [database_name] [container_name]
#
# Examples:
#   ./upgrade_module.sh                    # Upgrade foggy_mcp in odoo database
#   ./upgrade_module.sh foggy_mcp          # Upgrade foggy_mcp in odoo database
#   ./upgrade_module.sh foggy_mcp odoo     # Upgrade foggy_mcp in odoo database
#   ./upgrade_module.sh sale odoo odoo-17  # Upgrade sale in odoo database, custom container
#

set -e

# Default values
MODULE_NAME="${1:-foggy_mcp}"
DATABASE_NAME="${2:-odoo}"
CONTAINER_NAME="${3:-foggy-odoo}"
WAIT_SECONDS="${WAIT_SECONDS:-5}"

echo "=========================================="
echo "Odoo Module Upgrade Script"
echo "=========================================="
echo "Container:  ${CONTAINER_NAME}"
echo "Database:   ${DATABASE_NAME}"
echo "Module:     ${MODULE_NAME}"
echo "=========================================="

# Check if container exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container '${CONTAINER_NAME}' not found"
    echo "Available containers:"
    docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
    exit 1
fi

# Step 1: Stop container (avoids two Odoo processes competing)
echo ""
echo "[1/4] Stopping container '${CONTAINER_NAME}'..."
docker stop "${CONTAINER_NAME}"

# Step 2: Start container and wait for readiness
echo ""
echo "[2/4] Starting container and waiting ${WAIT_SECONDS}s..."
docker start "${CONTAINER_NAME}"
sleep "${WAIT_SECONDS}"

# Step 3: Upgrade module (runs a separate odoo process, then exits)
echo ""
echo "[3/4] Upgrading module '${MODULE_NAME}' in database '${DATABASE_NAME}'..."
docker exec "${CONTAINER_NAME}" bash -c "odoo -d ${DATABASE_NAME} -u ${MODULE_NAME} --stop-after-init"

# Step 4: Restart to load the upgraded module in the main Odoo process
echo ""
echo "[4/4] Restarting Odoo to apply changes..."
docker restart "${CONTAINER_NAME}"

echo ""
echo "=========================================="
echo "Module upgrade completed successfully!"
echo "Odoo is restarting — wait a few seconds before accessing the web UI."
echo "=========================================="
