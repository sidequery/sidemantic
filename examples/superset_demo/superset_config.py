"""Minimal Superset configuration for the sidemantic demo.

Uses SQLite for simplicity (built into the base Superset image).
"""

import os

# Security
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "superset-demo-secret-key-change-in-production")

# Use SQLite (built into the image, no extra drivers needed)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Feature flags for demo
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Disable CSRF for demo simplicity (not for production!)
WTF_CSRF_ENABLED = False
