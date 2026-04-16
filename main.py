import pandas as pd


def carregar_dados(caminho_sinasc, caminho_sim):
    print("Carregando bases...")

   
  
    temp_df = pd.read_csv(caminho_sinasc, nrows=1, sep=None, engine='python')
    print("Colunas encontradas no arquivo:", temp_df.columns.tolist())

    colunas_necessarias = [
        'PESO', 'SEMAGESTAC', 'RACACOR', 'GRAVIDEZ',
        'PARTO', 'IDADEMAE', 'SEXO', 'DTNASC', 'CODMUNNASC', 'APGAR5', 'IDANOMAL'
    ]
    sinasc = pd.read_csv(caminho_sinasc, usecols=colunas_necessarias, sep=None, engine='python')
    sim = pd.read_csv(caminho_sim, sep=None, engine='python')

    return sinasc, sim

# 2. Pré-processamento do Baseline
def pre_processamento_baseline(df_sinasc):
    print("Iniciando limpeza de outliers e nulos...")
    
    # Removendo valores nulos na coluna PESO 
    df_limpo = df_sinasc.dropna(subset=['PESO']).copy()
    
    # Definindo outliers
    q_low = df_limpo['PESO'].quantile(0.01)
    q_hi  = df_limpo['PESO'].quantile(0.99)
    
    df_final = df_limpo[(df_limpo['PESO'] > q_low) & (df_limpo['PESO'] < q_hi)]
    
    print(f"Registros originais: {len(df_sinasc)}")
    print(f"Registros após limpeza: {len(df_final)}")
    return df_final


sinasc_raw, sim_raw = carregar_dados('./bases/SINASC_2025.csv', './bases/DO25OPEN.csv')
sinasc_processado = pre_processamento_baseline(sinasc_raw)
