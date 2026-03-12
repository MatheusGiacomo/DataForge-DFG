import requests
from dfg.logger import logger

# Este modelo não depende de outros modelos locais
DEPENDENCIES = []

CONTRACT = {
    "id": ["not_null", "unique"],
    "email": ["not_null"],
}

def model(context):
    """
    Extrai usuários de uma API e mantém o estado do último ID processado.
    """
    # 1. Recupera o estado anterior (último ID processado)
    last_id = context.get("state") or 0
    
    url = "https://jsonplaceholder.typicode.com/users"
    logger.info(f"Buscando usuários na API a partir do ID > {last_id}...")
    
    response = requests.get(url)
    response.raise_for_status()
    all_users = response.json()

    # 2. Filtra apenas os "novos" dados (Lógica Incremental)
    new_users = [u for u in all_users if u['id'] > last_id]

    if not new_users:
        logger.info("Nenhum dado novo encontrado na API.")
        return []

    # 3. Formata os dados para o banco (Flattening básico)
    processed_data = []
    for u in new_users:
        processed_data.append({
            "id": u["id"],
            "name": u["name"],
            "email": u["email"],
            "city": u["address"]["city"], # Flattening de dicionário aninhado
            "company_name": u["company"]["name"]
        })

    # 4. Atualiza o estado com o maior ID encontrado nesta rodada
    max_id = max(u["id"] for u in processed_data)
    context["set_state"](max_id)
    
    logger.success(f"Extraídos {len(processed_data)} novos usuários.")
    return processed_data