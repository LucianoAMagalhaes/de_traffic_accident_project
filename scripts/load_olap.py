import pandas as pd
from sqlalchemy import create_engine, text

# Configuração do banco
db_config = {
    'user': 'postgres',
    'password': 'xperia',
    'host': 'localhost',
    'port': '5432',
    'database': 'dataengineering'
}

# Conectar ao banco
engine = create_engine(
    f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Criar as tabelas dimensionais e fato
create_tables_sql = """
DROP TABLE IF EXISTS fact_traffic_accidents;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_cause;

CREATE TABLE dim_cause (
    cause_id SERIAL PRIMARY KEY,
    cause_name TEXT UNIQUE
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT UNIQUE
);

CREATE TABLE fact_traffic_accidents (
    accident_id SERIAL PRIMARY KEY,
    cause_id INT REFERENCES dim_cause(cause_id),
    date_id INT REFERENCES dim_date(date_id),
    deaths INT
);
"""

with engine.connect() as conn:
    print("Create tables...")
    conn.execute(text(create_tables_sql))
    conn.commit()

# Ler dados da base bruta já carregada
df = pd.read_sql_table("traffic_accidents", con=engine)

# Cria e insere dim_causa
dim_cause = pd.DataFrame(df['Categoria'].dropna().unique(), columns=['cause_name'])
dim_cause.to_sql('dim_cause', con=engine, index=False, if_exists='append')
print("dim_cause populated.")

# Cria e insere dim_tempo
dim_date = pd.DataFrame(df['Year'].dropna().unique(), columns=['year'])
dim_date.to_sql('dim_date', con=engine, index=False, if_exists='append')
print("dim_date populated.")

# Recupera IDs para fazer joins
dim_cause_db = pd.read_sql_table("dim_cause", con=engine)
dim_date_db = pd.read_sql_table("dim_date", con=engine)

# Merge dos IDs nas tabelas
df_fact = df.merge(dim_cause_db, left_on='Categoria', right_on='cause_name')
df_fact = df_fact.merge(dim_date_db, left_on="Year", right_on="year")

# Mantém apenas colunas com os IDs e medidas
df_fact = df_fact[['cause_id', 'date_id', 'Deaths']]

# Inserir fato_obitos
df_fact.columns = df_fact.columns.str.lower()
df_fact.to_sql("fact_traffic_accidents", con=engine, index=False, if_exists='append')
#print("fact_traffic_accidents populated.")