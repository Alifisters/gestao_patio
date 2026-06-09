"""
=============================================================================
ETL - Relatório de Movimentação, Pátio e Portaria 
Autor: Aleff Andrade Costa
Descrição: Extração de dados da API SIAB, tratamento de tempos de 
           movimentação e carga no banco de dados do Google Sheets.
=============================================================================
"""
import json
import re
from datetime import timedelta, time, date, datetime

import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import os
# ==========================================
# 1. CONFIGURAÇÕES E CREDENCIAIS
# ==========================================
CPF_USUARIO = "70634444174"
SENHA_BS = 25654789
CREDENCIAL_JSON_SIAB = r"C:\Users\aleff\OneDrive\Área de Trabalho\Py\sharp-doodad-472618-r7-6deb50560b3f.json"

# URLs da API
BASE_URL = "https://business.comigo.com.br:45004"
WS_BUSINESS_URL = "https://servicesbusiness.comigo.com.br:45004/wsBusiness"

IDENTIFICAR_URL = f"{WS_BUSINESS_URL}/api/auth/identificarUsuario"
LOGIN_URL = f"{BASE_URL}/hub/login"
API_RELATORIO = f"{BASE_URL}/Administrativo/SIAB/GetRelatorioRP"
API_SIAB = f'{BASE_URL}/Administrativo/SIAB/GetRelatorioPatio'
URL_SIAB_MOV = f"{BASE_URL}/Administrativo/SIAB/RelatorioMovimentacoes"

CENTROS_ARMAZENS = [
    "0005", "0012", "0013", "0017", "0022", "0024", "0031", "0033",
    "0034", "0036", "0042", "0046", "0048", "0052", "0053", "0057",
    "0062", "0065"
]

# ==========================================
# 2. CÁLCULO DINÂMICO DE DATAS
# ==========================================
data_final = datetime.combine(date.today(), time.max)
data_inicial = datetime.combine(data_final - timedelta(days=7), time.min)

# Converte para milissegundos (o formato exigido pela API)
data_inicial_ms = int(data_inicial.timestamp() * 1000)
data_final_ms = int(data_final.timestamp() * 1000)

# Data para ticket por movimentação
data_ini_ticket = int(datetime.combine(
    date.today() - timedelta(days=7), time.max).timestamp() * 1000)
data_fim_ticket = int(datetime.combine(
    date.today(), time.min).timestamp() * 1000)

# ==========================================
# 3. FUNÇÕES AUXILIARES
# ==========================================


def convert_date(date_str):
    if isinstance(date_str, str):
        match = re.search(r'(\d+)', date_str)
        if match:
            timestamp = int(match.group(1))
            dt_object = pd.to_datetime(timestamp, unit='ms')
            return dt_object.strftime('%d/%m/%Y %H:%M:%S')
    return None


def extrair_horarios(lista_de_etapas_embarque):
    horario_dict = {}
    proxima_etapa_valor = ""

    for etapa in lista_de_etapas_embarque:
        descricao = etapa.get("Descricao")
        horario = etapa.get("Horario")
        proxima = etapa.get("proximaEtapa")

        if proxima and str(proxima).strip():
            proxima_etapa_valor = proxima

        if isinstance(horario, str) and descricao:
            nome_coluna = descricao.replace(
                " ", "_").replace("Ã", "A").replace("Ç", "C")
            horario_dict[nome_coluna] = horario

    horario_dict["proximaEtapa"] = proxima_etapa_valor
    return horario_dict


def formatar_hh_mm(td):
    if pd.isnull(td):
        return None
    total_seconds = td.total_seconds()
    sinal = '-' if total_seconds < 0 else ''
    total_seconds = abs(total_seconds)
    horas = int(total_seconds // 3600)
    minutos = int((total_seconds % 3600) // 60)
    return f'{sinal}{horas:02d}:{minutos:02d}'


# ==========================================
# 4. EXTRAÇÃO (EXTRACTION) - LOGIN E APIS
# ==========================================
session = requests.Session()

print("ETAPA 1: Identificando usuário...")
identificar_params = {'cpfCnpjOuEmail': CPF_USUARIO, 'isMobile': 'false'}

try:
    identificar_response = session.get(
        IDENTIFICAR_URL, params=identificar_params, timeout=30)
    identificar_response.raise_for_status()
    user_id = identificar_response.json().get('Parceiro', {}).get('Id')

    if not user_id:
        print("ERRO CRÍTICO: Não foi possível obter o ID do usuário.")
        exit()

    print(f"ID do usuário obtido com sucesso: {user_id}")

    print("\nETAPA 2: Realizando login...")
    login_payload = {'id': user_id,
                     'senha': SENHA_BS, 'credencial': CPF_USUARIO}
    login_response = session.post(LOGIN_URL, data=login_payload, timeout=30)
    login_response.raise_for_status()

    if "Login" in login_response.text:
        print("ERRO CRÍTICO: Falha no login (A página de login foi retornada).")
        exit()

    print("Login bem-sucedido!\n")

except Exception as e:
    print(f"Erro na requisição de parceiro ou login: {e}")
    exit()

print("ETAPA 3: Coletando dados do relatório RP...")
param_json = {
    "IDCentro": "", "DataInicial": data_inicial_ms, "DataFinal": data_final_ms,
    "HoraInicio": "", "HoraFinal": "", "RegistrosReimpressos": False,
    "Tipos": "2,6", "Placa": "", "De": "", "HoraMaiorIgual": "",
    "Ate": "", "Cancelados": False, "EmAberto": "on", "Fechados": "false",
    "CriadoPortaria": "on", "CriadoAutomaticamente": "on", "Reimpresso": False,
    "Transportador": ""
}
param_permissao = {"Acesso": "False", "Admin": "True"}

df_total = pd.DataFrame()

for centro in CENTROS_ARMAZENS:
    param_json["IDCentro"] = centro
    payload_relatorio = {
        "json": json.dumps(param_json),
        "Permissao": json.dumps(param_permissao)
    }
    response = session.get(API_RELATORIO, params=payload_relatorio)

    if response.status_code == 200:
        try:
            dados_brutos = response.json()
            df_relatorio = pd.json_normalize(dados_brutos)
            df_total = pd.concat([df_total, df_relatorio], ignore_index=True)
            print(f"Adicionado {len(df_relatorio)} linhas do centro {centro}.")
        except json.JSONDecodeError:
            print(f"Erro no centro {centro}: JSON inválido retornado.")
    else:
        print(
            f"Erro ao buscar relatório do centro {centro}. Código: {response.status_code}")

print("\nETAPA 4: Coletando dados do pátio interno...")
parametros_base = {
    "HoraInicio": "", "HoraFinal": "", "IDCentro": "", "IDDeposito": "0050",
    "IDProduto": "", "Tipo": "2,6", "DataInicial": data_inicial_ms,
    "DataFinal": data_final_ms, "Placa": ""
}

tabela_siab_df_total = pd.DataFrame()
for centro in CENTROS_ARMAZENS:
    parametros_base["IDCentro"] = centro
    parametros_relatorio = {
        "json": json.dumps(parametros_base),
        "Permissao": json.dumps(param_permissao)
    }
    print(f"Coletando dados do armazém {centro}...")
    response = session.get(API_SIAB, params=parametros_relatorio)
    try:
        tabela_siab = pd.json_normalize(response.json())
        tabela_siab_df_total = pd.concat(
            [tabela_siab_df_total, tabela_siab], ignore_index=True)
    except Exception as e:
        print(f"Erro ao converter JSON do pátio (Centro {centro}): {e}")

# ==========================================
# 5. TRANSFORMAÇÃO (TRANSFORMATION)
# ==========================================
if 'DataEntrada' in tabela_siab_df_total.columns:
    tabela_siab_df_total['DataEntrada'] = tabela_siab_df_total['DataEntrada'].apply(
        convert_date)

tabela_siab_df_total = tabela_siab_df_total.rename(columns={
    "DataEntrada": "Data Entrada",
    "EtapasConcluidas": "Concluido"
}).drop(columns=["Deposito", "SenhaPortaria", "HorarioAtual"], errors="ignore")

tabela_siab_df_total[['Cod Centro', 'Armazem']
                     ] = tabela_siab_df_total['Centro'].str.split('-', n=1, expand=True)
tabela_siab_df_total['Cod Centro'] = tabela_siab_df_total['Cod Centro'].astype(
    str).str.zfill(4)

tabela_etapas = tabela_siab_df_total["EtapasPesagem"].apply(extrair_horarios)
tabela_etapas_tratada = pd.json_normalize(tabela_etapas)

for coluna in tabela_etapas_tratada:
    if coluna != "proximaEtapa":
        tabela_etapas_tratada[coluna] = pd.to_datetime(
            tabela_etapas_tratada[coluna], errors="coerce", dayfirst=True)

tabela_final = pd.concat([tabela_siab_df_total.drop(
    columns=["EtapasPesagem"]), tabela_etapas_tratada], axis=1)
tabela_final = tabela_final.drop(columns="Centro", errors="ignore")

tabela_final = tabela_final.rename(columns={
    "CHECK-IN_PORTARIA": "Check-in Portaria",
    "ENTRADA_PORTARIA": "Entrada Portaria",
    "PESAGEM_DE_ENTRADA": "Pesagem de Entrada",
    "CLASSIFICACAO_DE_GRAOS": "Classificação de Grãos",
    "PESAGEM_SAÍDA": "Pesagem Saída",
    "SAÍDA_PORTARIA": "Saída Portaria",
    "proximaEtapa": "Próxima Etapa"
})

tabela_final_aberto = tabela_final[tabela_final["Concluido"] != True]

df_total["Patio Externo"] = ~df_total["Placa"].isin(
    tabela_final_aberto["Placa"])
df_total = df_total[df_total["Patio Externo"] != False]

patio_externo = df_total[["Placa", "Centro", "RP", "Patio Externo",
                          "TipoPesagem"]].rename(columns={"Centro": "Centro-Armazem"})
patio_externo[["Centro", "Armazem"]
              ] = patio_externo["Centro-Armazem"].str.split("-", n=1, expand=True)
patio_externo = patio_externo.drop(columns=["Centro-Armazem"])

print("\nETAPA 5: Coletando dados de movimentação...")
df_movimentos = pd.DataFrame()

for centro in CENTROS_ARMAZENS:
    print(f"Coletando tickets do centro {centro}")
    params_mov = {
        "HoraInicio": "", "HoraFinal": "", "IDCentro": centro, "IDDeposito": "0050",
        "IDProduto": "", "Tipo": "", "DataInicial": data_ini_ticket,
        "DataFinal": data_fim_ticket, "Transacionador": "", "Placa": ""
    }
    json_mov = {
        "json": json.dumps(params_mov),
        "Permissao": json.dumps(param_permissao)
    }
    response_movparcial = session.get(URL_SIAB_MOV, params=json_mov)
    try:
        dfparcial = pd.DataFrame(response_movparcial.json().get("Itens", []))
        df_movimentos = pd.concat(
            [df_movimentos, dfparcial], ignore_index=True)
    except Exception as e:
        print(f"Erro ao ler movimentação do centro {centro}: {e}")

df_movimentos[["Centro", "Armazem"]] = df_movimentos["Centro"].str.split(
    "-", n=1, expand=True)

colunas_mov = ["PesoBruto", "Tara", "PesoTotalBruto", "Descontos", "Acerto"]
for coluna in colunas_mov:
    if coluna in df_movimentos.columns:
        df_movimentos[coluna] = df_movimentos[coluna].astype("float64")

if 'DataEntrada' in df_movimentos.columns:
    df_movimentos["DataEntrada"] = pd.to_datetime(
        df_movimentos["DataEntrada"], dayfirst=True, unit="ms").dt.strftime(r"%d/%m/%Y")

remover_mov = [
    'DescontoUmidade', 'DescontoImpurezas', 'DescontoArdidos', 'DescontoQuebrados',
    'DescontoAvariados', 'DescontoEsverdeados', 'Status', 'Movimentos', 'Transgenia',
    'DescontoImpurezasPorc', 'DescontoUmidadePorc', 'DescontoArdidosPorc',
    'DescontoVerdesPorc', 'DescontoVerdes', 'DescontoCarunchadoPorc', 'DescontoCarunchado',
    'DescontoQuebradosPorc', 'DescontoMofadosPorc', 'DescontoMofados', 'DescontoAvariadosPorc',
    'PesoLiquido', 'DescricaoMoega', 'DescricaoArmazem'
]
df_movimentos = df_movimentos.drop(
    columns=[col for col in remover_mov if col in df_movimentos.columns])

df_movimentos = df_movimentos.rename(columns={
    "DataEntrada": "Data Entrada", "TipoPesagem": "Tipo Pesagem",
    "PesoBruto": "Peso Bruto", "PesoTotalBruto": "Peso Total Bruto",
    "PesoTotalLiquido": "Peso Total Liquido"
})

if "Pesagem Saída" in tabela_final.columns:
    tabela_final["Pesagem Saída"] = tabela_final["Pesagem Saída"].dt.strftime(
        date_format=r"%d/%m/%Y")

df_movimentos = df_movimentos.merge(
    right=tabela_final[["Cod Centro", "Ticket", "Pesagem Saída"]],
    how="left", left_on=["Centro", "Ticket"], right_on=["Cod Centro", "Ticket"]
)
if "Cod Centro" in df_movimentos.columns:
    df_movimentos.pop("Cod Centro")

tabela_final = tabela_final[tabela_final["Concluido"] != True]

# ==========================================
# PADRONIZANDO OS NOMES PARA PERMITIR FILTRO
# ==========================================
tabela_final["Transacionador"] = (tabela_final["Transacionador"]
                                  .str.upper()
                                  .str.replace("/", " ", regex=False)
                                  .str.replace(".", " ", regex=False)
                                  .str.replace("  ", " ", regex=False))
df_movimentos["Transacionador"] = (df_movimentos["Transacionador"]
                                   .str.upper()
                                   .str.replace("/", " ", regex=False)
                                   .str.replace(".", " ", regex=False)
                                   .str.replace("  ", " ", regex=False))
# ==========================================
# 6. CARGA (LOAD) - GOOGLE SHEETS
# ==========================================
print("\nETAPA 6: Enviando dados para o Google Sheets...")
try:
    cliente = gspread.service_account(filename=CREDENCIAL_JSON_SIAB)
    geral = cliente.open("Dados Mestre")

    aba_siab = geral.worksheet("Rel_Siab")
    aba_siab.clear()
    set_with_dataframe(worksheet=aba_siab, dataframe=tabela_final, resize=True)
    print(f"Planilha {aba_siab.title} atualizada!")

    patio_ext = geral.worksheet("Patio_externo")
    patio_ext.clear()
    set_with_dataframe(worksheet=patio_ext,
                       dataframe=patio_externo, resize=True)
    print(f"Planilha {patio_ext.title} atualizada!")

    mov_ticket = geral.worksheet("Mov_ticket")
    mov_ticket.clear()
    set_with_dataframe(worksheet=mov_ticket,
                       dataframe=df_movimentos, resize=True)
    print(f"Planilha {mov_ticket.title} atualizada!\n")

    print("🚀 PROCESSO FINALIZADO COM SUCESSO!")

except Exception as e:
    print(f"Erro ao salvar no Google Sheets: {e}")
