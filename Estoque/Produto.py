import psycopg2
from faker import Faker
import random
from decimal import Decimal

fake = Faker('pt_BR')

# Dados simulados para o nosso "PRODUTOS"
tipos_produtos = ['Alimentação', 'Limpeza', 'Higiene', 'Bebidas', 'Hortifruti']
produtos = {
    'Alimentação': ['Arroz 5kg', 'Feijão 1kg', 'Macarrão Espaguete', 'Óleo de Soja', 'Açúcar Refinado 1kg', 'Café 500g'],
    'Limpeza': ['Sabão em Pó 1kg', 'Detergente Líquido', 'Amaciante 2L', 'Desinfetante', 'Água Sanitária', 'Esponja'],
    'Higiene': ['Sabonete', 'Creme Dental', 'Shampoo', 'Condicionador', 'Papel Higiênico', 'Desodorante'],
    'Bebidas': ['Refrigerante Cola 2L', 'Suco de Uva Integral', 'Água Mineral 500ml', 'Cerveja Lata', 'Vinho Tinto'],
    'Hortifruti': ['Maçã', 'Banana', 'Tomate', 'Cebola', 'Batata', 'Alface']
}

categorias = [
    {'nome': 'Alimentação', 'percentual_imposto': 0.05},
    {'nome': 'Limpeza', 'percentual_imposto': 0.15},
    {'nome': 'Higiene', 'percentual_imposto': 0.12},
    {'nome': 'Bebidas', 'percentual_imposto': 0.20},
    {'nome': 'Hortifruti', 'percentual_imposto': 0.02}
]

def gerar_produto():
    """Gera dados aleatórios para um único produto"""
    categoria = random.choice(categorias)
    nome_produto = random.choice(produtos[categoria['nome']])
    
    # Gera um código de barras EAN-13 fictício
    codigo_barra = fake.ean13()
    
    # Gera um preço unitário entre R$ 1.50 e R$ 45.00
    preco_unitario = round(random.uniform(1.50, 45.00), 2)
    
    # Quantidade variável (ex: de 1 a 500 itens em estoque)
    quantidade = random.randint(1, 500)
    
    # Data de fabricação: até 1 ano atrás
    data_fabricacao = fake.date_between(start_date='-1y', end_date='today')
    
    # Data de validade: de hoje até 2 anos no futuro
    data_validade = fake.date_between(start_date='today', end_date='+2y')
    
    return (
        codigo_barra, 
        nome_produto, 
        categoria['nome'], 
        preco_unitario, 
        categoria['percentual_imposto'],
        data_fabricacao,
        data_validade,
        quantidade
    )

def salvar_no_banco(qtd_registros):
    conexao = None
    try:
        conexao = psycopg2.connect(
            user="postgres",
            password="0542",
            host="localhost",
            port="5432",
            database="Data_lake_supermercado",
            options="-c lc_messages=C"
        )
        
        # Agora podemos mudar para UTF-8 para os dados, se necessário
        conexao.set_client_encoding('UTF8')
        
        cursor = conexao.cursor()
        print("Conectado ao PostgreSQL com sucesso!")

        # Comando SQL de inserção
        sql_insert = """
            INSERT INTO produtos (codigo_barra, nome_produto, categoria, preco_unitario, percentual_imposto, data_fabricacao, data_validade, quantidade)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Gera os dados e executa a inserção
        for i in range(qtd_registros):
            dados_produto = gerar_produto()
            cursor.execute(sql_insert, dados_produto)
            
            if (i + 1) % 50 == 0:
                print(f"{i + 1} produtos gerados e inseridos...")

        # Salva permanentemente no banco
        conexao.commit()
        print(f"\nFinalizado! {qtd_registros} produtos foram cadastrados com sucesso.")

    except (Exception, psycopg2.DatabaseError) as error:
        # Agora sim, se o banco reclamar de algo, a mensagem virá limpa!
        print(f"\nERRO REAL IDENTIFICADO: {error}")
    finally:
        if conexao is not None:
            cursor.close()
            conexao.close()
            print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    salvar_no_banco(20)