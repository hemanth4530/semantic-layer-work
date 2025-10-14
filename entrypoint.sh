#!/bin/bash
set -e

# Step 1: Run the initialization script
python -m app.introspect_to_catalog

# Step 2: Launch Streamlit app on port 8504
streamlit run app/ui_streamlit.py --server.port=8504
