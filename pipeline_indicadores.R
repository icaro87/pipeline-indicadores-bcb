# pipeline_indicadores.R

library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(DBI)
library(RPostgres)
library(tidyr)

# --- Função para buscar dados da API SGS
buscar_sgs <- function(codigo_serie, data_inicio, data_fim) {
  url <- paste0(
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.",
    codigo_serie,
    "/dados?formato=json&dataInicial=", data_inicio, "&dataFinal=", data_fim
  )
  resposta <- tryCatch({
    GET(url)
  }, error = function(e) return(NULL))
  
  if (is.null(resposta) || resposta$status_code != 200) {
    return(tibble::tibble(data = as.Date(NA), valor = NA_real_))
  }
  
  dados <- tryCatch({
    fromJSON(content(resposta, "text", encoding = "UTF-8"))
  }, error = function(e) return(tibble::tibble(data = as.Date(NA), valor = NA_real_)))
  
  if (length(dados) == 0 || nrow(dados) == 0) {
    return(tibble::tibble(data = as.Date(NA), valor = NA_real_))
  }
  
  dados %>%
    mutate(data = dmy(data),
           valor = as.numeric(gsub(",", ".", valor)))
}

# --- Lista de indicadores desejados
indicadores <- tibble::tribble(
  ~codigo, ~nome,
  433, "IPCA",
  4189, "SELIC",
  189, "IGPM",
  7326, "PIB",
  1, "USD",
  188, "INCC",
  4506, "DIVIDA_PUBLICA"
)

# --- Conexão com o banco Neon
con <- dbConnect(
  Postgres(),
  dbname = "neondb",
  host = "ep-late-fog-actpcfvk-pooler.sa-east-1.aws.neon.tech",
  port = 5432,
  user = "neondb_owner",
  password = Sys.getenv("NEON_PW"),
  sslmode = "require"
)

# --- Verifica se a tabela existe
tabela_existe <- dbExistsTable(con, "indicadores_economicos")

# --- Define a data de início
if (tabela_existe) {
  ultima_data <- dbGetQuery(con, "SELECT MAX(data) FROM indicadores_economicos")[[1]]
  data_inicio <- format(ultima_data + 1, "%d/%m/%Y")
} else {
  data_inicio <- "01/01/2024"
}

data_fim <- format(Sys.Date(), "%d/%m/%Y")

# --- Buscar e combinar dados
todos_dados <- indicadores %>%
  rowwise() %>%
  mutate(dados = list(
    buscar_sgs(codigo, data_inicio, data_fim) %>%
      mutate(indicador = nome)
  )) %>%
  select(dados) %>%
  unnest(dados)

dados_prontos <- todos_dados %>%
  select(data, indicador, valor) %>%
  arrange(data, indicador)

# --- Criar tabela se necessário
if (!tabela_existe) {
  dbCreateTable(con, "indicadores_economicos", dados_prontos)
}

# --- Inserir dados
if (nrow(dados_prontos) > 0) {
  dbAppendTable(con, "indicadores_economicos", dados_prontos)
  message("✅ Dados inseridos com sucesso.")
} else {
  message("ℹ️ Nenhum novo dado para inserir.")
}

dbDisconnect(con)
