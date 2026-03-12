# models/stg_users.py

# Contrato para validação
CONTRACT = {
    "id": ["not_null", "unique"],
    "nome": ["not_null"]
}

def model(context):
    last_id = context["state"] or 0
    
    # Simula dados novos
    new_data = [{"id": last_id + 1, "nome": "Novo Usuario"}]
    
    # Se houver dados, atualiza o estado
    if new_data:
        context["set_state"](new_data[-1]["id"])
    
    return new_data