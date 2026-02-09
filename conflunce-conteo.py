import os
import sys
import requests
import urllib.parse

# --- CONFIGURACIÓN ---
BASE_URL  = os.getenv("CONFLUENCE_BASE_URL", "https://tudominio.atlassian.net/wiki/")
EMAIL     = os.getenv("ATLASSIAN_EMAIL", "tu-email@dominio.com")
API_TOKEN = os.getenv("ATLASSIAN_API_TOKEN", "tu-api-token")

# Escribe aquí parte del título que ves en la URL (sin los símbolos +)
TARGET_TITLE = "ANX-Apigee to AWS API Gateway Migration In process"

if not BASE_URL.endswith("/"): BASE_URL += "/"
if "/wiki/" not in BASE_URL: BASE_URL = BASE_URL.rstrip("/") + "/wiki/"

session = requests.Session()
session.auth = (EMAIL, API_TOKEN)
session.headers.update({"Accept": "application/json"})

def get_my_id():
    try:
        return session.get(BASE_URL + "rest/api/user/current").json().get("accountId")
    except:
        return None

def force_space_admin(space_key, account_id):
    if not space_key: return
    print(f"   -> Elevando permisos en espacio '{space_key}'...")
    url = BASE_URL + f"rest/api/space/{space_key}/permission"
    payload = {
        "subject": {"type": "user", "identifier": account_id},
        "operation": {"key": "administer", "target": "space"}
    }
    try:
        session.post(url, json=payload)
    except:
        pass

def nuke_page(page_id, title):
    print(f"   >>> DESBLOQUEANDO ID REAL: {page_id} ({title})")
    url = BASE_URL + f"rest/api/content/{page_id}/restriction"
    payload = [
        {"operation": "read", "restrictions": {"user": [], "group": []}},
        {"operation": "update", "restrictions": {"user": [], "group": []}}
    ]
    try:
        r = session.put(url, json=payload)
        if r.status_code == 200:
            print("       [EXITO] Candado eliminado.")
        else:
            print(f"       [FALLO] {r.status_code} - {r.text}")
    except Exception as e:
        print(f"       [ERROR] {e}")

def search_by_title():
    print(f"--- BUSCANDO POR TÍTULO: '{TARGET_TITLE}' ---")
    
    # Usamos CQL para buscar por título (incluyendo borradores y archivados)
    # Sintaxis: title ~ "texto"
    cql = f'title ~ "{TARGET_TITLE}"'
    encoded_cql = urllib.parse.quote(cql)
    
    # Buscamos en contenido normal y en papelera
    search_url = BASE_URL + f"rest/api/content/search?cql={encoded_cql}&expand=space,version,history&limit=10"
    
    try:
        r = session.get(search_url)
        data = r.json()
        results = data.get("results", [])
        
        if not results:
            print("[X] No se encontró ninguna página con ese título.")
            print("    Prueba acortando el título en la variable TARGET_TITLE.")
            return

        print(f"[!] Se encontraron {len(results)} coincidencia(s).")
        my_id = get_my_id()
        
        for item in results:
            real_id = item.get("id")
            real_title = item.get("title")
            status = item.get("status")
            space_key = item.get("space", {}).get("key")
            
            print(f"\n   ------------------------------------------------")
            print(f"   Encontrado: {real_title}")
            print(f"   ID Real:    {real_id}")
            print(f"   Estado:     {status}")
            print(f"   Espacio:    {space_key}")
            
            # Acción
            force_space_admin(space_key, my_id)
            nuke_page(real_id, real_title)

    except Exception as e:
        print(f"[ERROR CRITICO] {e}")

if __name__ == "__main__":
    search_by_title()
