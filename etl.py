#ETL
#Utilizando o pyenv, temos o numpy nesse arquivo
#Ferramenta de processamento: Pandas - Polars - Duckdb - Spark - Dask
#Ferramenta de qualidade: Pydantic (Linha a Linha ou uma API) - Pandera (SQL, DF)
import pandas as pd
import os
import glob

"""
Uma função de Extract que lê e consolida os jsons
"""
def extrair_dados_e_concatenar(path: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*.json'))
    #Percorre os arquivos json e armazena num data frame
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

"""
Uma função de Transform que 
"""
def calcular_kpo_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df
"""
Uma função que dá load em csv ou parquet
"""
def carregar_dados_para_csv_ou_parquet(df: pd.DataFrame, format: list):
    for formato in format:
        if formato == "csv":
            df.to_csv("dados.csv", index=False)
        elif formato == "parquet":
            df.to_parquet("dados.parquet")
        else:
            print("Formato não encontrado")

"""
Chamando todas as funções da pipeline:
"""
def pipeline_calcular_kpi_vendas(path: str, formato_de_saida):
    #path = 'data'
    df_concatenado = extrair_dados_e_concatenar(path)
    df_atualizado_com_total = calcular_kpo_de_total_de_vendas(df=df_concatenado)
    #formato_de_saida: list = ['csv']
    carregar_dados_para_csv_ou_parquet(df=df_atualizado_com_total,
                                        format=formato_de_saida)
#O ideal é colocar isso abaixo em outro arquivo .py
if __name__ == "__main__":
    pasta = 'data'
    df_concatenado = extrair_dados_e_concatenar(path=pasta)
    df_atualizado_com_total = calcular_kpo_de_total_de_vendas(df=df_concatenado)
    formato_de_saida: list = ['csv']
    carregar_dados_para_csv_ou_parquet(df=df_atualizado_com_total,
                                        format=formato_de_saida)
