# bees

Projeto desenvolvido para Case de recrutamento para o BEES  
Para execução é necessário clonar o repositório em algum workspace databricks

Origem dos Dados: https://api.openbrewerydb.org/v1/breweries

Projeto construído seguindo arquitetura medalhão - Camadas Bronze, Silver, Gold

Tabelas criadas em ambiente de DataLakehouse do Databricks  
Unity Catalog: bees  
Database: brewery  

Tabela Bronze: brewery_bronze  
```
Schema:
  id STRING,  
  name STRING, 
  brewery_type STRING,
  address_1 STRING,
  address_2 STRING,
  address_3 STRING,
  city STRING,
  state_province STRING,
  postal_code STRING,
  country STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  phone STRING,
  website_url STRING,
  state STRING,
  street STRING
```

Tabela Silver: brewery_silver  
```
Schema:
  id STRING,  
  name STRING, 
  brewery_type STRING,
  address_1 STRING,
  address_2 STRING,
  address_3 STRING,
  city STRING,
  state_province STRING,
  postal_code STRING,
  country STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  phone STRING,
  website_url STRING,
  data_quality STRUCT
      is_address_missing BOOLEAN
      is_location_missing BOOLEAN
      is_lat_long_missing BOOLEAN
      is_phone_missing BOOLEAN
      is_website_missing BOOLEAN
```

View GOLD: vw_brewery_type_location
```
Schema:
  brewery_type STRING,  
  country STRING, 
  total INT
```

Check Data Quality  
Criada Coluna data_quality na tabela brewery_silver, o qual realiza verificação de missing values nas colunas do dataframe:
- is_address_missing: Colunas avaliadas (address_1, address_2, address_3, postal_code)
- is_location_missing: Colunas avaliadas (city, state_province, country)
- is_lat_long_missing: Colunas avaliadas (longitude, latitude)
- is_phone_missing: Colunas avaliadas (phone)
- is_website_missing: Colunas avaliadas (website_url)

Criada view vw_brewery_silver_quality, que retorna uma contagem de ocorrencias de cada check e o seu percentual de impacto sobre todos os registros da tabela.  
Esta view é utilizada para configurar os Alertas de notificação do Data Quality (Brewery_silver_check_quality)

Orquestração  
-Utilizado o databricks workflow para execução dos notebooks (Bronze, Siver, Gold)  
-Schedule: diariamente às 06 horas  
-Notificação por email em caso de falha do pipeline  
-Retries: 1x cada task - delay 5 min

Alerta Data Quality  
Criado o Brewery_silver_check_quality utilizando o recurso de Alertas do databricks, onde é executada uma query a partir da view vw_brewery_silver_quality.  
Execuções diárias às 06:30  
Enviado uma notificação por email para as seguintes ocorrências:
- Se qualquer check de data_quality representar 100% dos registros da tabela
- Se check location_missing for maior que 0% da tabela  


