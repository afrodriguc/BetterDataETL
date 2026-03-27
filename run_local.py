# run_local.py — Ejecuta cualquier función localmente cargando el .env
from dotenv import load_dotenv
load_dotenv()  # Carga el .env antes que todo

import sys
import os

# Agregar shared/src al path para que etl_carriers sea importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'shared', 'src'))

func = sys.argv[1] if len(sys.argv) > 1 else "manual_etl_loader"

if func == "manual_etl_loader":
    from functions.manual_etl_loader.main import main
    main()
elif func == "bronze_to_silver":
    from functions.bronze_to_silver.main import main
    main()