# models/fct_sales.py

DEPENDENCIES = ["stg_users"]

CONTRACT = {
    "sale_id": ["not_null", "unique"],
    "user_id": ["not_null"]
}

def model(context):
    # Busca o retorno do modelo stg_users
    users = context["ref"]("stg_users")
    
    # DEFESA: Se stg_users veio vazio, fct_sales também deve retornar vazio
    if not users:
        return []
        
    # Agora é seguro acessar o índice [0]
    return [{"sale_id": 101, "user_id": users[0]["id"]}]