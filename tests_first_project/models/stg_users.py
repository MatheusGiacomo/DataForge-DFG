def model(context):
    last_id = context["state"] or 0
    logger = f"Buscando dados a partir do ID: {last_id}"
    
    # Simula dados novos
    new_data = [{"id": last_id + 1, "nome": "Novo Usuario"}]
    
    # Salva o novo estado para a próxima rodada
    context["set_state"](new_data[-1]["id"])
    
    return new_data