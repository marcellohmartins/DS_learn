# Atividade Spark SQL

Considerando o dataset detalhado a seguir, extraia o conjunto de informações solicitadas.

### Dataset dados da BOVESPA

- Arquivo disponível em /home/dados/bovespa/bovespa.csv
- Dados relativos a bovespa, a bolsa de valores no Brasil
- ~1.3GB
- 8.1M de instâncias


| #  	| Nome do campo                                                             	| Descrição                                                                        	|
|----	|---------------------------------------------------------------------------	|----------------------------------------------------------------------------------	|
| 0  	| RegisterType                                                             	| Fixo '1'                                                                                 	|
| 1  	| TradingDate                                                              	| Data do pregão                                                                   	|
| 2  	| BDICode                                                                  	| Utilizado para classificar os papéis na emissão do boletim diário de informações 	|
| 3  	| NegociationCode                                                          	| Codigo de negociação do papel                                                    	|
| 4  	| MarketType                                                               	| Cód. Do mercado em que o papel está cadastrado                                   	|
| 5  	| TradeName                                                                	| Nome resumido da empresa emissora do papel                                       	|
| 6  	| Specification                                                             	| Especificação do Papel                                                           	|
| 7  	| ForwardMarketTermInDays                                               	| Prazo em dias do mercado a termo                                                 	|
| 8  	| Currency                                                                  	| Moeda de referência                                                              	|
| 9  	| OpeningPrice                                                             	| Preço de abertura do papel no pregão                                             	|
| 10 	| MaxPrice                                                                	| Preço máximo do papel no pregão                                                  	|
| 11 	| MinPrice                                                                	| Preço mínimo do papel no pregão                                                  	|
| 12 	| MeanPrice                                                                	| Preço médio do papel no pregão                                                   	|
| 13 	| LastTradePrice                                                          	| Preço do último negócio do papel no pregão                                       	|
| 14 	| BestPurshaseOrderPrice                                                 	| Preço da melhor oferta de compra do papel no mercado                             	|
| 15 	| BestPurshaseSalePrice                                                  	| Preço da melhor oferta de venda do papel no mercado                              	|
| 16 	| NumborOfTrades                                                          	| Número de negócios efetuados com o papel no pregão                               	|
| 17 	| NumberOfTradedStocks                                                   	| Quantidade total de títulos negociados neste papel                               	|
| 18 	| VolumeOfTradedStocks                                                   	| Volume total de títulos negociados neste papel                                   	|
| 19 	| PriceForOptionsMarketOrSecondaryTermMarket                         	| Preço de exercício para o mercado de opções ou valor do contrato para o mercado  	|
| 20 	| PriceCorrectionsForOptionsMarketOrSecondaryTermMarket             	| Indicador de correção de preços de exercícios ou valores de contrato             	|
| 21 	| DueDateForOptionsMarketOrSecondaryTermMarket                      	| Data do vencimento para os mercados de opções                                    	|
| 22 	| FactorOfPaperQuotatuion                                                	| Fator de cotação do papel                                                        	|
| 23 	| PointsInPriceForOptionsMarketReferencedInDollarOrSecondaryTerm 	| Preço de exercício em pontos para opções referenciadas em dólar                  	|
| 24 	| ISINOrInternCode                                                       	| Código do papel no sistema ISIN                                                  	|
| 25 	| DistributionNumber                                                       	| Número de distribuição do papel                                                  	|

Informações a serem extraídas:

1. Quantidade de dias com operações da PETR4 (NegociationCode)
2. Maior valor (MaxPrice) histórico por ação (NegociationCode)
3. Maior valor (MaxPrice) histórico da PETR4 (NegociationCode)
4. Dia ('TradingDate') com a maior quantidade de papeis (NegociationCode) operados
5. Dia ('TradingDate') da semana com a maior quantidade de papeis (NegociationCode) operados
6. Maior lucro histórico de um papel (NegociationCode) na bovespa (MaxPrice - OpeningPrice)
7. Maior prejuizo histórico de um papel (NegociationCode) na bovespa (OpeningPrice - LastTradePrice)
8. Moeda (Currency) com mais operações
9. Papel (NegociationCode) operado em CZ (Currency) com maior quantidade de operações
10. Papel (NegociationCode) operado em CZ (Currency) com maior valor médio das operações (MeanPrice)
11. Media do preço médio (MeanPrice), mínimo (MinPrice) e máximo (MaxPrice) anual (TradingDate) das ações da PETR4 (NegociationCode)
12. Preço médio (MeanPrice) anual (TradingDate) das ações da PETR4 (NegociationCode)
13. Preço médio (MeanPrice) anual (TradingDate) das 10 ações (NegociationCode) com mais operações na bovespa
14. Desvio Padrão anual do preço médio (MeanPrice) da ação da PETR4 (NegociationCode)
15. Desvio Padrão anual do preço médio (MeanPrice) das 10 ações (NegociationCode) com mais operações na bovespa
16. Preço médio (MeanPrice) anual (TradingDate) das ações (NegociationCode) com a maior quantidade de operações de acordo com a moeda (Currency)


**Dicas:**
- *Crie uma célula (Insert -> Insert Cell Below) para cada informação solicitada*
- *A análise deve ser feita sobre os dados do HDFS*
- *Inicialize o seu cluster executando o script em: Desktop/ambientes/spark/inicializar.sh*
- *Acesse o seu cluster executando o script em: Desktop/ambientes/spark/abrir_navegador.sh*

import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .master('spark://spark-master:7077') \
    .config('spark.executor.memory', '1g') \
    .getOrCreate()

df = sc.read \
    .option('delimiter', ',') \
    .option('header', 'true') \
    .option('inferschema', 'true') \
    .csv('hdfs://namenode:9000/bovespa.csv')

df.printSchema()

import pyspark.sql.functions as func

df.select(func.col('TradingDate')).show(5)

df.select(func.col('TradingDate'), func.col('NegociationCode'))\
    .filter((func.col('TradingDate')=='19860102') & (func.col('NegociationCode')=='ACE 2'))\
    .show(5)

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

udfPegaAno = udf(lambda tradingDate: str(tradingDate)[0:4], StringType())

df.select(func.col('TradingDate'))\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .orderBy(func.col('ano'), ascending=False)\
    .show(5)

df.select(func.col('TradingDate'))\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .select(func.col('ano'))\
    .groupBy(func.col('ano'))\
    .agg(func.count(func.col('ano')).alias('quantidade'),
        func.max(func.col('ano')).alias('ValorMaximo'),
        func.stddev(func.col('ano')).alias('desvpad'))\
    .show(5)

resultado = df.select(func.col('TradingDate'))\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .select(func.col('ano'))\
    .groupBy(func.col('ano'))\
    .agg(func.count(func.col('ano')).alias('quantidade'))\
    .orderBy(func.col('ano'))\
    .collect()

listaAno = [x['ano'] for x in resultado]
listaQT = [x['quantidade'] for x in resultado]

import matplotlib.pyplot as plt

plt.plot(listaAno, listaQT)

df.createOrReplaceTempView('tabela')
sc.sql('select TradingDate from tabela').show(5)





#informacao 1

df.select(func.col('NegociationCode'))\
    .filter(func.col('NegociationCode')=='PETR4')\
    .count()

#informacao 2

df.select(func.col('NegociationCode'), func.col('MaxPrice'))\
    .groupBy(func.col('NegociationCode'))\
    .agg(func.max(func.col('MaxPrice')).alias('ValorMaximo'))\
    .orderBy(func.col('ValorMaximo'), ascending=False)\
    .show(5)

#informacao 3

df.select(func.col('NegociationCode'), func.col('MaxPrice'))\
    .filter(func.col('NegociationCode')=='PETR4')\
    .groupBy(func.col('NegociationCode'))\
    .agg(func.max(func.col('MaxPrice')).alias('ValorMaximo'))\
    .show(5)

#informacao 4 - Dia ('TradingDate') com a maior quantidade de papeis (NegociationCode) operados

df.select(func.col('TradingDate'), func.col('NegociationCode'))\
    .groupBy(func.col('TradingDate'))\
    .agg(func.count(func.col('NegociationCode')).alias('papeis'))\
    .orderBy(func.col('papeis'), ascending=False)\
    .show(1)

from datetime import datetime

udfDiaSemana = udf(lambda x: datetime.strptime(str(x), '%Y%m%d').isocalendar()[2], StringType())

#informacao 5 - Dia ('TradingDate') da semana com a maior quantidade de papeis (NegociationCode) operados
df.select(func.col('TradingDate'))\
    .withColumn('weekDay', udfDiaSemana(func.col('TradingDate')))\
    .select(func.col('weekDay'))\
    .groupBy(func.col('weekDay'))\
    .agg(func.count(func.col('weekDay')).alias('papeisOperados'))\
    .orderBy(func.col('papeisOperados'), ascending=False)\
    .show(7)

#informacao 6 - Maior lucro histórico de um papel (NegociationCode) na bovespa (MaxPrice - OpeningPrice)

udfLucro = udf(lambda maxPrice, openingPrice: maxPrice - openingPrice)


df.select(func.col('NegociationCode'), func.col('MaxPrice'), func.col('OpeningPrice'))\
    .withColumn('Lucro', udfLucro(func.col('MaxPrice'), func.col('OpeningPrice')))\
    .orderBy(func.col('Lucro'), ascending=False)\
    .show(1)



#informacao 7 - Maior prejuizo histórico de um papel (NegociationCode) na bovespa (OpeningPrice - LastTradePrice)

udfPrejuizo = udf(lambda openingPrice, LastTradePrice: openingPrice - LastTradePrice)


df.select(func.col('NegociationCode'), func.col('OpeningPrice'), func.col('LastTradePrice'))\
    .withColumn('Preju', udfPrejuizo(func.col('OpeningPrice'), func.col('LastTradePrice')))\
    .orderBy(func.col('Preju'), ascending=False)\
    .show(1)

#informacao 8 - Moeda (Currency) com mais operações

df.select(func.col('Currency'))\
    .groupBy(func.col('Currency'))\
    .agg(func.count(func.col('Currency')).alias('Moeda'))\
    .orderBy(func.col('Currency'), ascending=False)\
    .show(1)

#informacao 9 - Papel (NegociationCode) operado em CZ (Currency) com maior quantidade de operações

df.select(func.col('NegociationCode'), func.col('Currency'))\
    .filter(func.col('Currency')=='CZ$')\
    .groupBy(func.col('NegociationCode'))\
    .agg(func.count(func.col('NegociationCode')).alias('Total'))\
    .orderBy(func.col('Total'), ascending=False)\
    .show(1)

#informacao 10 - Papel (NegociationCode) operado em CZ (Currency) com maior valor médio das operações (MeanPrice)

df.select(func.col('NegociationCode'), func.col('Currency'), func.col('MeanPrice'))\
    .filter(func.col('Currency')=='CZ$')\
    .groupBy(func.col('NegociationCode'))\
    .agg(func.max(func.col('MeanPrice')).alias('Media'))\
    .orderBy(func.col('Media'), ascending=False)\
    .show(5)

#informacao 11 - Media do preço médio (MeanPrice), mínimo (MinPrice) e máximo (MaxPrice) 
#anual (TradingDate) das ações da PETR4 (NegociationCode)

df.select(func.col('NegociationCode'), func.col('MeanPrice'), func.col('MinPrice'), func.col('MaxPrice'), func.col('TradingDate'))\
    .filter(func.col('NegociationCode')=='PETR4')\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .groupBy(func.col('Ano'))\
    .agg(func.mean(func.col('MeanPrice')).alias('MediaAno'),
        func.mean(func.col('MinPrice')).alias('MinAno'),
        func.mean(func.col('MaxPrice')).alias('MaxAno'))\
    .orderBy(func.col('Ano'), ascending=False)\
    .show(5)

#informacao 12

df.select(func.col('NegociationCode'), func.col('MeanPrice'), func.col('TradingDate'))\
    .filter(func.col('NegociationCode')=='PETR4')\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .groupBy(func.col('Ano'))\
    .agg(func.mean(func.col('MeanPrice')).alias('MediaAno'))\
    .orderBy(func.col('Ano'), ascending=False)\
    .show(5)



#informacao 13 - Preço médio (MeanPrice) anual (TradingDate) das 10 ações (NegociationCode) com 
#mais operações na bovespa
resultadoTop10 = df.select(func.col('NegociationCode'))\
    .groupBy(func.col('NegociationCode'))\
    .count()\
    .orderBy(func.col('count'), ascending = False)\
    .take(10)

papelTop10 = [x['NegociationCode'] for x in resultadoTop10]
papelTop10

df.filter(df.NegociationCode.isin(papelTop10))\
    .select(func.col('NegociationCode'), func.col('TradingDate'), func.col('MeanPrice'))\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .groupBy(func.col('NegociationCode'), func.col('ano'))\
    .agg(func.mean(func.col('MeanPrice')).alias('precoMedio'))\
    .show(10)

#informacao 14

df.select(func.col('NegociationCode'), func.col('MeanPrice'), func.col('TradingDate'))\
    .filter(func.col('NegociationCode')=='PETR4')\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .groupBy(func.col('Ano'))\
    .agg(func.stddev(func.col('MeanPrice')).alias('stdDevAno'))\
    .orderBy(func.col('Ano'), ascending=False)\
    .show(5)

#informacao 15 - Desvio Padrão anual do preço médio (MeanPrice) das 10 ações (NegociationCode) 
#com mais operações na bovespa

df.filter(df.NegociationCode.isin(papelTop10))\
    .select(func.col('NegociationCode'), func.col('TradingDate'), func.col('MeanPrice'))\
    .withColumn('ano', udfPegaAno(func.col('TradingDate')))\
    .groupBy(func.col('NegociationCode'), func.col('ano'))\
    .agg(func.stddev(func.col('MeanPrice')).alias('stdDevPrice'))\
    .show(10)

#informacao 16 - Preço médio (MeanPrice) anual (TradingDate) das ações (NegociationCode) 
# com a maior quantidade de operações de acordo com a moeda (Currency)
