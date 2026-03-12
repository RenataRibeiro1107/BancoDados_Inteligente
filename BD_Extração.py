import pandas as pd
from datetime import datetime
import json
from conexao_consulta import get_connection, execute_query
from inserir_Muitos_Valores import insert_many_values
from dotenv import load_dotenv
import psycopg2
import math
import os

# Carregar variáveis de ambiente do arquivo dados_conexao.env 
load_dotenv("dados_conexao.env", override=True)

# Configuração da conexão com o banco de dados
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Instalar no Anaconda PowerShell - pip install python-dotenv

# Validação de variáveis de ambiente
for key, value in DB_CONFIG.items():
    if value is None:
        raise ValueError(f"Variável de ambiente não definida: {key}")

#######
# Controle de carga
#######

# Início de uma execução de carga na tabela de controle stg.controle_carga 
def iniciar_carga(conn, processo, entidade, arquivo):

    # Inserção de um registro na tabela de controle_carga (nome do processo, tabela, data/hora de inicio, status, nome do arquivo de origem)
    query = """
        INSERT INTO stg.controle_carga
        (processo_nome, entidade, data_inicio, status, arquivo_origem)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING carga_id
    """
    # Execução do comando SQL - O status muda para SUCESSO quando a execução da carga é finalizada (depois que todas as validações foram realizadas, nenhuma exceção foi identificada, as inserções ocorreram e o commit (gravação) foi realizado)
    return execute_query(
        conn, query,
        (processo, entidade, datetime.now(), "EM_EXECUCAO", arquivo)
    )[0][0]

# Término da execução de carga - status final (SUCESSO ou ERRO)
def finalizar_carga(conn, carga_id, status, mensagem=None,
                    lidos=0, inseridos=0, atualizados=0, erro=0):
    query = """
        UPDATE stg.controle_carga
        SET data_fim = %s,
            status = %s,
            mensagem = %s,
            registros_lidos = %s,
            registros_inseridos = %s,
            registros_atualizados = %s,
            registros_erro = %s
        WHERE carga_id = %s
    """
    execute_query(
        conn, query,
        (datetime.now(), status, mensagem,
         lidos, inseridos, atualizados, erro, carga_id),
        # A consulta não retorna dados - UPDATE
        return_data=False
    )

# Registro na tabela de erro_carga de erro identificado durante a execução de carga 
def registrar_erro(conn, carga_id, entidade, etapa, dado, motivo):
    #Registro do id da carga que apresentou erro, nome da entidade, etapa, dado de origem e motivo do erro)
    query = """
        INSERT INTO stg.erro_carga
        (carga_id, entidade, etapa, dado_origem, motivo_erro)
        VALUES (%s, %s, %s, %s, %s)
    """
    execute_query(
        conn, query,
        (carga_id, entidade, etapa, json.dumps(limpar_nan(dado)), motivo),
        return_data=False
    )

#######
# Ajustes
#######

# Substitui valores NaN (python) por None (null no DW)
def limpar_nan(obj):
    #Se for um dicionário (conjunto de valores)
    if isinstance(obj, dict):
        return {k: limpar_nan(v) for k, v in obj.items()}
    #Se for uma lista
    if isinstance(obj, list):
        return [limpar_nan(v) for v in obj]
    #Se for float (se o atributo  for numeric e estiver sem valor)
    if isinstance(obj, float) and math.isnan(obj):
        return None

    return obj

# Normalização de valores
def limpar_valor(valor):

    # Trata NaN e None
    if pd.isna(valor):
        return None

    # Converte NumPy → Python nativo
    if hasattr(valor, "item"):
        valor = valor.item()

    # Trata string com vírgula decimal
    if isinstance(valor, str):
        valor = valor.strip()

        if (
            "," in valor
            and valor.replace(",", "").replace(".", "").isdigit()
        ):
            valor = valor.replace(".", "").replace(",", ".")

        # Tenta converter para número
        try:
            valor = float(valor)
        except ValueError:
            return valor

    # Trata números float
    if isinstance(valor, float):

        # Remove inf, -inf
        if not math.isfinite(valor):
            return None

        # Se for inteiro disfarçado (2.0 → 2)
        if valor.is_integer():
            return int(valor)

        return valor

    return valor

# Garante que o código IBGE de um município é numérico e tem 7 dígitos - realiza o ajusta caso o código apresente 6 dígitos - verifica no conjunto de códigos válidos (atributo municipio_cod_ibge) e retorna o código correto
def corrigir_codigo_ibge(cod_ibge, ibges_validos: set[int]):
    #Se for vazio
    if cod_ibge is None:
        return None
    # Remoção de espaços, zero a esquerda. Se for uma string retorna None
    try:
        cod = int(float(str(cod_ibge).strip()))
    except Exception:
        return None

    cod_str = str(cod)

    # Verifica se tem 7 dígitos e existe na tabela municipio
    if len(cod_str) == 7 and cod in ibges_validos:
        return cod

    # Existem 6 dígitos (falta o  último) - Realiza correção
    if len(cod_str) == 6:
        #Criação de uma lista vazia para armazenar possíveis IBGEs corretos encontrados no DW
        candidatos = [i for i in ibges_validos
                     if str(i).startswith(cod_str)]

        #Se encontrou um candidato, retorna o valor
        if len(candidatos) == 1:
            return int(candidatos[0])

    return None

#######
# Qualidade dos dados
#######

# Completude das chaves primárias - as chaves naturias obrigatórias são não nulas e não vazias 
def validar_completude_chaves_primaria(row, campos):
    for campo in campos:
        if row[campo] is None or str(row[campo]).strip() == "":
            return False, campo
    return True, None

# Detecção de outliers - método do Intervalo Interquartil (IQR)
def detectar_outliers_iqr(df, coluna):
    #Remove valores nulos da coluna
    serie = df[coluna].dropna()
    #Se tiver menos que 10 dados é muito pouco e confiável para realizar o cálculo
    if len(serie) < 10:
        return pd.DataFrame()
    #Primeiro e terceiro quartil
    q1 = serie.quantile(0.25)
    q3 = serie.quantile(0.75)
    iqr = q3 - q1
    #Limites do outlier
    limite_inf = q1 - 1.5 * iqr
    limite_sup = q3 + 1.5 * iqr
    #Retorna as linhas problemáticas
    return df[(df[coluna] < limite_inf) | (df[coluna] > limite_sup)]

# Quebra abrupta de série temporal
def detectar_quebra_serie(df, coluna_valor, limite_pct=0.5):

    df_ord = df.sort_values("ano").copy()

    # Calcula variação percentual
    df_ord["variacao"] = df_ord[coluna_valor].pct_change()

    # Remove infinitos (divisão por zero)
    df_ord["variacao"].replace(
        [float("inf"), float("-inf")],
        float("nan"),
        inplace=True
    )

    # Filtra apenas onde existe valor válido
    return df_ord[
        df_ord["variacao"].notna() &
        (df_ord["variacao"].abs() > limite_pct)
    ]

# Dados brutos -> tabelas de staging -> tabelas do banco de dados
# Carga incremental: apenas dados novos ou alterados são carregados; e não a tabela inteira

#######
# Carga incremental das tabelas de staging
#######

#Carregar os dados do dataframe para a tabela de staging apropriada - conexão, dataframe, tabela de staging e colunas que serão inseridas
def carregar_staging(conn, df: pd.DataFrame, tabela: str, colunas: list):

    valores = [
        tuple(limpar_valor(valor) for valor in linha)
        for linha in df[colunas].itertuples(index=False, name=None)
    ]

# Inserir em batches para não sobrecarregar memória
    batch_size = 1500  # ajustar conforme o volume de dados
    for i in range(0, len(valores), batch_size):
        batch = valores[i:i+batch_size]
        insert_many_values(conn, tabela, colunas, batch)

    if not valores:
        return

#######
# Carga incremental das tabelas do banco de dados
####### 

# Tabela município
def carregar_municipio(conn):
    execute_query(conn, """
    INSERT INTO bd.municipio
    (municipio_cod_ibge, municipio_nome, estado_nome,
     estado_sigla,municipio_regiao)
    SELECT DISTINCT
        s.municipio_cod_ibge,
        s.municipio_nome,
        s.estado_nome,
        s.estado_sigla,
        s.municipio_regiao
    FROM stg.municipio s
    WHERE s.municipio_cod_ibge IS NOT NULL
    ORDER BY s.municipio_cod_ibge
    ON CONFLICT (municipio_cod_ibge) DO NOTHING;
    """, return_data=False)

# Tabela variável
def carregar_variavel(conn):
    execute_query(conn, """
        INSERT INTO bd.variavel
        (variavel_sigla, variavel_nome, variavel_fonte, variavel_tipo)
        SELECT DISTINCT s.variavel_sigla, s.variavel_nome, s.variavel_fonte, s.variavel_tipo
        FROM stg.variavel s
        WHERE s.variavel_sigla IS NOT NULL
        ORDER BY s.variavel_sigla
        ON CONFLICT (variavel_sigla) DO NOTHING;
    """, return_data=False)

# Tabela municipio_apresenta_variavel
def carregar_municipio_apresenta_variavel(conn):
    execute_query(conn, """
        INSERT INTO bd.municipio_apresenta_variavel
        (
            municipio_cod_ibge,
            variavel_sigla,
            ano,
            variavel_valor
        )
        SELECT
            m.municipio_cod_ibge,
            v.variavel_sigla,
            f.ano,
            f.variavel_valor
        FROM stg.municipio_apresenta_variavel f
        JOIN bd.municipio m
            ON m.municipio_cod_ibge = f.municipio_cod_ibge
        JOIN bd.variavel v
            ON v.variavel_sigla = f.variavel_sigla
        ORDER BY 
            m.municipio_cod_ibge,
            f.ano DESC
        ON CONFLICT (municipio_cod_ibge, variavel_sigla, ano) DO NOTHING;
    """, return_data=False)

#######
# Validação
#######

# Transformação estrutural - padronização do nome das colunas das tabelas
def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
          .str.strip() # remove espaços no início e fim
          .str.lower() # minúsculo
          .str.replace(" ", "_") # troca espaço por underscore
          .str.replace("-", "_") # troca hífen por underscore
    )
    return df

# Integridade estrutural - verifica se o arquivo contém todas as colunas obrigatórias da tabela
def validar_colunas(df: pd.DataFrame, colunas_esperadas: list, entidade: str):
    faltantes = set(colunas_esperadas) - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Arquivo da entidade '{entidade}' não possui colunas obrigatórias: {faltantes}"
        )

# Consulta a tabela de municipio para recuperar os códigos IBGE que já existem no banco de dados 
# Dados são números
def carregar_ibges_validos(conn) -> set[int]:
    """
    Retorna conjunto de códigos IBGE válidos (int)
    """
    rows = execute_query(
        conn,
        "SELECT municipio_cod_ibge FROM bd.municipio"
    )
    return {int(r[0]) for r in rows if r[0] is not None}

    
#######
# Validação das tabelas 
#######

# Tabela municipio_apresenta_variavel
def validar_municipio_apresenta_variavel(
    conn,
    df,
    carga_id,
    ibges_validos: set[int]
):
    erros = 0
    correcoes = 0
    
    # Conjunto de todas as variavel_sigla existentes na tabela variavel
    variaveis_validas = {
        r[0].lower() for r in execute_query(
            conn,
            "SELECT variavel_sigla FROM bd.variavel"
        )
    }

    chaves = ['municipio_cod_ibge', 'variavel_sigla', 'ano']

    dup = df[df.duplicated(subset=chaves, keep=False)]

    if not dup.empty:
        raise ValueError("Duplicidades encontradas nas chaves naturais")

    # Validações realizadas linha a linha
    for _, r in df.iterrows():

        cod_ibge = r['municipio_cod_ibge']
        sigla_var = r['variavel_sigla']
        ano = r['ano']
        valor = r['variavel_valor']

        # Completude das chaves primárias - verifica se todas as chaves primárias existem
        ok, campo = validar_completude_chaves_primaria(
            r,
            ['municipio_cod_ibge', 'variavel_sigla', 'ano']
        )

        if not ok:
            registrar_erro(conn, carga_id,
                'municipio_apresenta_variavel',
                'COMPLETUDE',
                r.to_dict(),
                campo
            )
            erros += 1
            continue

        # Correção e validação do código IBGE
        cod_corrigido = corrigir_codigo_ibge(cod_original, ibges_validos)

        if cod_corrigido is None:
            registrar_erro(conn, carga_id,
                'municipio_apresenta_variavel',
                'QUALIDADE',
                r.to_dict(),
                f'Código IBGE inválido ({cod_ibge})'
            )
            erros += 1
            continue
            
        # Caso apresente 6 dígitos 
        # Conversão do valor para string
        cod_original = str(cod_ibge).strip()

        # Tentativa de correção quando apresenta 6 dígitos - falta o último dígito
        cod_corrigido = corrigir_codigo_ibge(cod_original, ibges_validos)
        
        # Verifica se houve correção
        if cod_original != str(cod_corrigido):
            registrar_erro(
                conn, carga_id,
                'municipio_apresenta_variavel',
                'CORRECAO_AUTOMATICA',
                r.to_dict(),
                f'IBGE corrigido automaticamente de {cod_ibge} para {cod_corrigido}'
            )
            correcoes += 1 
               
        # Atualização do valor do código IBGE diretamente no df com a versão corrigida - substituição do código original (possivelmente errado) pelo código validado e padronizado antes de enviar para staging.
        df.at[r.name, 'municipio_cod_ibge'] = cod_corrigido
    
        # Verifica se a variável existe na tabela variavel
        if sigla_var.lower() not in variaveis_validas:

            registrar_erro(conn, carga_id,
                'municipio_apresenta_variavel',
                'VALIDACAO',
                r.to_dict(),
                f'Variável inexistente ({sigla_var})'
            )
            erros += 1
            continue

    # Detecção de outliers
    for _, r in detectar_outliers_iqr(df, 'variavel_valor').iterrows():
        registrar_erro(conn, carga_id,
            'municipio_apresenta_variavel',
            'OUTLIER',
            r.to_dict(),
            'Outlier detectado'
        )

    # Quebra de série temporal
    for _, r in detectar_quebra_serie(df, 'variavel_valor').iterrows():
        registrar_erro(conn, carga_id,
            'municipio_apresenta_variavel',
            'SERIE',
            r.to_dict(),
            'Quebra abrupta'
        )

    return erros, correcoes


#######
# Execução do processo ETL (Extract, Transform, Load)
#######

def executar_etl_excel(caminho_excel: str, entidade: str):
    # Abre a conexão com o banco de dados - PostgreSQL
    conn = get_connection(**DB_CONFIG)

    # Inserção de um registro na tabela de controle de carga
    carga_id = iniciar_carga(
        conn,
        processo=f"CARGA_{entidade.upper()}",
        entidade=entidade.upper(),
        arquivo=caminho_excel
    )

    registros_lidos = 0 # quantidade de registros lidos
    registros_inseridos = 0 # quantidade de registros inseridos
    erros = 0 # quantidade de erros
    correcoes = 0 # quantidade de correções realizadas

    try:

        # Leitura do arquivo Excel - pega apenas a primeira aba
        dfs = pd.read_excel(caminho_excel, sheet_name=None, dtype=str)
        df = next(iter(dfs.values()))
        registros_lidos = len(df)

        # Normalização estrutural
        df = normalizar_colunas(df)
        entidade = entidade.lower()
        
        # Tabelas - tipos de entidades
           
        # Tabela município
        # Verifica qual entidade está sendo carregada. Por exemplo, se chamar executar_etl_excel("arquivo.xlsx", "municipio") este bloco será executado
        if entidade == "municipio":

            #df - são os possíveis nomes que estão nas colunas do arquivo Excel
            # Padronização dos nomes das colunas
            df = df.rename(columns={
                "codigo_ibge": "municipio_cod_ibge",
                "municipio": "municipio_nome",
                "estado": "estado_nome",
                "uf": "estado_sigla",
                "regiao": "municipio_regiao"
            })
            # Colunas obrigatórias
            colunas = [
                "municipio_cod_ibge",
                "municipio_nome",
                "estado_nome",
                "estado_sigla",
                "municipio_regiao"
            ]
            # Validação estrutural - o arquivo Excel contém todas as colunas obrigatórias
            validar_colunas(df, colunas, entidade)    

            df_validos = []
            erros = 0
            correcoes = 0
            # Linha a linha
            for _, r in df.iterrows():
                # Validação de completude
                ok, campo = validar_completude_chaves_primaria(
                    r, ['municipio_cod_ibge']
                )

                if not ok:
                    registrar_erro(
                        conn, carga_id,
                        "MUNICIPIO",
                        "COMPLETUDE",
                        r.to_dict(),
                        f"Campo obrigatório ausente: {campo}"
                    )
                    erros += 1
                    continue
                # DataFrame apenas com registros válidos    
                df_validos.append(r.to_dict())
            df = pd.DataFrame(df_validos, columns=colunas)

            # Inserção dos dados transformados na tabela de staging 
            carregar_staging(conn, df, "stg.municipio", colunas)
            # Inserção dos dados transformados na tabela municipio
            carregar_municipio(conn)
            
        # Tabela variavel
        elif entidade == "variavel":
            df = df.rename(columns={
                "sigla": "variavel_sigla",
                "nome": "variavel_nome",
                "fonte": "variavel_fonte",
                "tipo": "variavel_tipo"
            })
            
            colunas = [
                "variavel_sigla",
                "variavel_nome",
                "variavel_fonte",
                "variavel_tipo"
            ]

            validar_colunas(df, colunas, entidade)
            carregar_staging(conn, df, "stg.variavel", colunas)
 
            try: # A mesma chave primária (variavel_sigla) não é inserida mais de uma vez (AG01 = ag01) devido a criação de índice no PostgreSQL
                carregar_variavel(conn)
            except psycopg2.errors.UniqueViolation as e:
                conn.rollback()

                mensagem_erro = (
                    "Sigla da variável já existente no bd.com grafia diferente "
                    "(violação case-insensitive). Registro não inserido."
                )

                print("\n ERRO DE DUPLICIDADE NA TABELA VARIAVEL")
                print(mensagem_erro)
                print("Detalhe técnico:", str(e))
                print("--------------------------------------------------\n")

                registrar_erro(
                    conn,
                    carga_id,
                    "VARIAVEL",
                    "DUPLICIDADE_CASE_INSENSITIVE",
                    {},
                    mensagem_erro
                )

                erros += 1
                
# Tabelas - tipos de relacionamento

        elif entidade.startswith("municipio"):

            df = df.rename(columns={
                "codigo_ibge": "municipio_cod_ibge",
                "sigla": "variavel_sigla",
            })

            rows = execute_query(
                conn,
                "SELECT municipio_cod_ibge FROM bd.municipio"
            )

            ibges_validos_int = {int(r[0]) for r in rows if r[0] is not None}
           
            # municipio_apresenta_variavel
            if entidade == "municipio_apresenta_variavel":

                colunas = [
                    "municipio_cod_ibge",
                    "variavel_sigla",
                    "ano",
                    "variavel_valor",
                ]

                validar_colunas(df, colunas, entidade)

                #Se o valor_variavel for NULL troca por zero
                df["variavel_valor"] = df["variavel_valor"].fillna(0)
    
    
                municipios_validos = set(
                    int(row[0]) for row in execute_query(
                        conn, "SELECT municipio_cod_ibge FROM bd.municipio"
                    )
                )
                                
                variaveis_validas = set(
                    row[0] for row in execute_query(
                        conn, "SELECT variavel_sigla FROM bd.variavel"
                    )
                )
                
                df_validos = []
                erros = 0
                
                ibges_validos = municipios_validos

                for _, r in df.iterrows():
                
                    if (
                        pd.isna(r["municipio_cod_ibge"]) or
                        pd.isna(r["variavel_sigla"]) or
                        pd.isna(r["ano"])
                    ):
                        registrar_erro(
                            conn, carga_id,
                            "municipio_apresenta_variavel",
                            "COMPLETUDE",
                            r.to_dict(),
                            "Chave obrigatória ausente"
                        )
                        erros += 1
                        continue               
                    
                    cod_original = r["municipio_cod_ibge"]
                    cod_corrigido = corrigir_codigo_ibge(cod_original, ibges_validos)

                    if cod_corrigido is None:
                        registrar_erro(
                            conn, carga_id,
                            "municipio_apresenta_variavel",
                            "QUALIDADE",
                            r.to_dict(),
                            f"Código IBGE inválido ({cod_original})"
                        )
                        erros += 1
                        continue

                    r["municipio_cod_ibge"] = cod_corrigido

                    if cod_corrigido not in municipios_validos:
                        registrar_erro(
                            conn, carga_id,
                            "municipio_apresenta_variavel",
                            "MUNICIPIO_INEXISTENTE",
                                r.to_dict(),
                        "Município não existe na tabela municipio"
                        )
                        erros += 1
                        continue

                    if r["variavel_sigla"] not in variaveis_validas:
                        registrar_erro(
                            conn, carga_id,
                            "municipio_apresenta_variavel",
                            "VARIAVEL_INEXISTENTE",
                            r.to_dict(),
                            "Variável não existe na tabela variavel"
                        )
                        erros += 1
                        continue

                    df_validos.append(r.to_dict())                
                df = pd.DataFrame(df_validos, columns=colunas)

                if erros > 0:
                    print(f"{erros} registros inválidos foram enviados para stg.erro_carga")

                carregar_staging(
                    conn, df,
                    "stg.municipio_apresenta_variavel",
                    colunas
                )

                carregar_municipio_apresenta_variavel(conn)

#######
# Finalização da carga
#######

        finalizar_carga(
            conn,
            carga_id,
            status="SUCESSO",
            mensagem=f"Carga executada com sucesso. {correcoes} correções automáticas realizadas.",
            lidos=registros_lidos,
            inseridos=registros_lidos - erros,
            erro=erros
        )

    except Exception as e:

        finalizar_carga(
            conn,
            carga_id,
            status="ERRO",
            mensagem=str(e),
            lidos=registros_lidos,
            inseridos=0,
            erro=1
        )
        raise

    finally:
        conn.close()

#######
# Execução
#######

if __name__ == "__main__":
    caminho_excel = "nomeArquivo.xlsx"
    executar_etl_excel(caminho_excel, "municipio_apresenta_variavel")

