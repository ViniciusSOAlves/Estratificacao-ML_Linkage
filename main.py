import pandas as pd
import splink.duckdb.comparison_library as cl
from splink.duckdb.linker import DuckDBLinker

# FUNCAO DE CARREGAMENTO E LIMPEZA 
def preparar_dados(caminho_sinasc, caminho_sim):
    print("Iniciando carregamento e limpeza...")
    
    # Colunas essenciais identificadas no baseline
    colunas_necessarias = [
        'PESO', 'SEMAGESTAC', 'RACACOR', 'GRAVIDEZ', 
        'PARTO', 'IDADEMAE', 'SEXO', 'DTNASC', 'CODMUNNASC', 'APGAR5', 'IDANOMAL'
    ]
    
    # Carregamento com detetor de separador 
    sinasc = pd.read_csv(caminho_sinasc, usecols=colunas_necessarias, sep=None, engine='python')
    sim = pd.read_csv(caminho_sim, sep=None, engine='python')
  
    sinasc["unique_id"] = range(len(sinasc))
    sim["unique_id"] = range(len(sim))
    
    # Limpeza de Outliers
    sinasc = sinasc.dropna(subset=['PESO'])
    q_low = sinasc['PESO'].quantile(0.01)
    q_hi  = sinasc['PESO'].quantile(0.99)
    sinasc_limpo = sinasc[(sinasc['PESO'] > q_low) & (sinasc['PESO'] < q_hi)].copy()
    
    print(f"SINASC pronto: {len(sinasc_limpo)} registros.")
    return sinasc_limpo, sim

# CONFIGURACAO DO MODELO DE LINKAGE 
def executar_linkage(df_sinasc, df_sim):
    print("Configurando o motor de Record Linkage...")
    
    settings = {
        "link_type": "link_only", 
        "blocking_rules_to_generate_predictions": [
            "l.DTNASC = r.DTNASC and l.CODMUNNASC = r.CODMUNNASC"
        ],
        "comparisons": [
            cl.exact_match("PESO", term_frequency_adjustments=True),
            cl.exact_match("SEMAGESTAC"),
            cl.exact_match("SEXO"),
            cl.exact_match("IDADEMAE"),
            cl.exact_match("CODMUNNASC")
        ],
        "retain_matching_columns": True
    }

    linker = DuckDBLinker([df_sinasc, df_sim], settings, input_table_names=["sinasc", "sim"])
    
  
    print("Estimando pesos das colunas (isso pode demorar)...")
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.estimate_parameters_using_expectation_maximization("l.DTNASC = r.DTNASC")
    linker.estimate_parameters_using_expectation_maximization("l.PESO = r.PESO")
    
  
    print("Executando predição final...")
    df_matches = linker.predict(threshold_match_probability=0.95).as_pandas_frame()
    
    return df_matches
