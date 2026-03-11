DEPENDENCIES = ["stg_users"] # Força stg_users a rodar antes

def model(context):
    # O context["ref"] busca o retorno do modelo anterior no cache
    users = context["ref"]("stg_users")
    return [{"sale_id": 101, "user_id": users[0]["id"]}]