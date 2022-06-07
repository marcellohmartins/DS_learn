# Atividade Aprendizagem de Máquina - Spark MLLib

Considerando o dataset detalhado a seguir, aplique as técnicas solicitadas

### Dataset dados de sensores veicular

- Arquivo disponível em /home/dados/sensores/treinamento.csv
- Arquivo disponível em /home/dados/sensores/teste.csv
- Dados relativos a sensores de internet das coisas (IoT) para detecção de estados dos medidores


| #  	| Nome do campo           	| Descrição                                                                     	|
|----	|-------------------------	|-------------------------------------------------------------------------------	|
| 0  	| Hora               	| Hora média das medições                                                          	|
| 1  	| Minuto         	| Minuto médio das medições                                               	|
| 2  	| Temp_minima         	| Temperatura mínima das medições                                               	|
| 3  	| Temp_maxima         	| Temperatura máxima das medições                                               	|
| 4  	| Latitude_media  	| Latitude média das medições                             	|
| 5  	| Longitude_media  	| Longitude média das medições                             	|
| 6  	| Classe  	| Estado do medidor (Frio, Moderado, Quente, Alerta)                             	|


**Informações a serem extraídas:**

1. Calcule a acurácia de classificação na base de testes para os seguintes classificadores:
    1. Árvore de Decisão (from pyspark.ml.classification import DecisionTreeClassifier)
    2. Random Forest com 5 arvores (from pyspark.ml.classification import RandomForestClassifier, e numTrees=5 no construtor do RandomForestClassifier)
    3. Random Forest com 100 arvores (numTrees=100 no construtor do RandomForestClassifier)
2. Determine qual a quantidade de eventos Alerta (label = 3.0) classificados erroneamente como outra classe (falso-negativo) para os classificadores
    1. Árvore de Decisão (from pyspark.ml.classification import DecisionTreeClassifier)
    2. Random Forest com 5 arvores (from pyspark.ml.classification import RandomForestClassifier, e numTrees=5 no construtor do RandomForestClassifier)
    3. Random Forest com 100 arvores (numTrees=100 no construtor do RandomForestClassifier)
3. Determine qual a quantidade de eventos não Alerta (label = 0.0, ou label = 1.0, ou label = 2.0) classificados erroneamente como classe Alerta (falso-positivo) para os classificadores
    1. Árvore de Decisão (from pyspark.ml.classification import DecisionTreeClassifier)
    2. Random Forest com 5 arvores (from pyspark.ml.classification import RandomForestClassifier, e numTrees=5 no construtor do RandomForestClassifier)
    3. Random Forest com 100 arvores (numTrees=100 no construtor do RandomForestClassifier)
4. Faça votação entre os classificadores da etapa 1.A, 1.B e 1.C para atribuir a classe do evento de acordo com a maioria das classes entre os classificadores
    - Dicas: Para isto, voce irá precisar fazer o join das predições de cada classificador de acordo com os IDs dos eventos. Posteriormente voce pode manipular o dataframe, após o join, para determinar qual classe de cada evento possuiu maior votação =). Exemplo de código:
```python

    import pyspark.sql.functions as func
    
    predicaoDT.select(func.col('prediction').alias('prediction_dt'),
                        func.col('label'),
                        func.col('id'))\
    .join(predicaoRF.select(func.col('prediction').alias('prediction_rf'),
                        func.col('id')), ['id'])
```
5. Considerando que voce possui apenas duas classes: Não Alerta e Alerta. Calcule a acurácia de classificação na base de testes para os seguintes classificadores:
    1. Árvore de Decisão (from pyspark.ml.classification import DecisionTreeClassifier)
    2. Random Forest com 20 arvores (from pyspark.ml.classification import RandomForestClassifier, e numTrees=20 no construtor do RandomForestClassifier)
    3. Random Forest com 100 arvores (numTrees=100 no construtor do RandomForestClassifier)
        - Dicas: Para isto, você irá precisar manipular o dataframe para alterar os valores da coluna label, por exemplo através de uma UDF
6. Determine qual a quantidade de eventos Alerta (label = 3.0) classificados erroneamente como outra classe (falso-negativo) para os classificadores do item 5
7. Faça busca de parametros dos classificadores desenvolvidos no item 5. Plote um gráfico relacionando a acurácia e os parametros otimizados
    1. Árvore de Decisão varie o parametro maxDepth de 1 a 20
    2. Random Forest varie o numTrees de 1 a 20

**Dicas:**
- *Crie uma célula (Insert -> Insert Cell Below) para cada informação solicitada*
- *A análise deve ser feita sobre os dados do HDFS*
- *Inicialize o seu cluster executando o script em: Desktop/ambientes/spark/inicializar.sh*
- *Acesse o seu cluster executando o script em: Desktop/ambientes/spark/abrir_navegador.sh*


import os
os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

from pyspark.sql import SparkSession

sc = SparkSession\
    .builder\
    .master('spark://spark-master:7077')\
    .config('spark.executor.memory','1g')\
    .getOrCreate()

dfTreino = sc.read\
    .option('delimiter',',')\
    .option('header','true')\
    .option('inferschema','true')\
    .csv('hdfs://namenode:9000/treinamento.csv')

dfTeste = sc.read\
    .option('delimiter',',')\
    .option('header','true')\
    .option('inferschema','true')\
    .csv('hdfs://namenode:9000/teste.csv')

dfTreino.printSchema()

from pyspark.sql.functions import monotonically_increasing_id

dfTeste = dfTeste.withColumn('id', monotonically_increasing_id())
dfTeste.show(5)

from pyspark.ml.feature import StringIndexer, VectorAssembler

features = ['hora', 'minuto', 'temp_minima', 'temp_maxima', 'latitude_media', 'longitude_media']

#cria y
dfTreino = StringIndexer(inputCol='Classe', outputCol='label')\
    .fit(dfTreino)\
    .transform(dfTreino)

#cria x
dfTreino = VectorAssembler(inputCols=features, outputCol='features')\
    .transform(dfTreino)

dfTreino.show(5)

from pyspark.ml.feature import StringIndexer, VectorAssembler

features = ['hora', 'minuto', 'temp_minima', 'temp_maxima', 'latitude_media', 'longitude_media']

dfTeste = StringIndexer(inputCol='Classe', outputCol='label')\
    .fit(dfTeste)\
    .transform(dfTeste)

dfTeste = VectorAssembler(inputCols=features, outputCol='features')\
    .transform(dfTeste)

#informação 1.A
from pyspark.ml.classification import DecisionTreeClassifier
import pyspark.sql.functions as func

dt = DecisionTreeClassifier(labelCol='label', featuresCol='features')
modeloDT = dt.fit(dfTreino)

predicaoDT = modeloDT.transform(dfTeste)
predicaoDT.select(func.col('prediction'), func.col('label'), func.col('id')).show(5)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
    .evaluate(predicaoDT)

#informação 1.B
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=5)
modeloRF = rf.fit(dfTreino)

predicaoRF = modeloRF.transform(dfTeste)
predicaoRF.select(func.col('prediction'), func.col('label'), func.col('id')).show(5)

MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
    .evaluate(predicaoRF)

#informação 1.C

rfCem = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
modeloRFCEM = rfCem.fit(dfTreino)
predicaoRFCEM = modeloRFCEM.transform(dfTeste)
predicaoRFCEM.select(func.col('prediction'), func.col('label'), func.col('id')).show(5)

MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
    .evaluate(predicaoRFCEM)

# Determine qual a quantidade de eventos Alerta (label = 1.0) 
# classificados erroneamente como outra classe (falso-negativo) para os classificadores

predicaoDT.select(func.col('label'), func.col('Classe')).distinct().show(5)

#informação 2.A
predicaoDT.printSchema()

predicaoDT.filter(func.col('Classe') == 'Alerta')\
    .filter(func.col('prediction')!= func.col('label'))\
    .count() / predicaoDT.filter(func.col('Classe') == 'Alerta').count()



#informação 2.B

predicaoRF.filter(func.col('Classe') == 'Alerta')\
    .filter(func.col('prediction')!= func.col('label'))\
    .count() / predicaoRF.filter(func.col('Classe') == 'Alerta').count()



#informação 2.C


predicaoRFCEM.filter(func.col('Classe') == 'Alerta')\
    .filter(func.col('prediction')!= func.col('label'))\
    .count() / predicaoDT.filter(func.col('Classe') == 'Alerta').count()

# etermine qual a quantidade de eventos não Alerta (label = 0.0, ou label = 1.0, ou label = 2.0) 
# classificados erroneamente como classe Alerta (falso-positivo) para os classificadores

#informação 3.A

predicaoDT.filter(func.col('Classe') != 'Alerta')\
    .filter(func.col('prediction')== 1)\
    .count() / predicaoDT.filter(func.col('Classe') != 'Alerta').count()





#informação 3.B

predicaoRF.filter(func.col('Classe') != 'Alerta')\
    .filter(func.col('prediction')== 1)\
    .count() / predicaoDT.filter(func.col('Classe') != 'Alerta').count()



#informação 3.C

predicaoRFCEM.filter(func.col('Classe') != 'Alerta')\
    .filter(func.col('prediction')== 1)\
    .count() / predicaoDT.filter(func.col('Classe') != 'Alerta').count()

#informação 4

# Faça votação entre os classificadores da etapa 1.A, 1.B e 1.C para atribuir a classe do evento de acordo 
# com a maioria das classes entre os classificadores

import pyspark.sql.functions as func
from statistics import mode

def pegaMaiorOcorrencia(x):
    try:
        return [x[0], mode(x[1])]
    except:
        return [x[0], x[2]]

predicaoDT.select(func.col('prediction').alias('prediction_dt'),
                  func.col('label'),
                  func.col('id'))\
    .join(predicaoRF.select(func.col('prediction').alias('prediction_rf'),
                  func.col('id')), ['id'])\
    .join(predicaoRFCEM.select(func.col('prediction').alias('prediction_rf_cem'),
                  func.col('id')), ['id'])\
    .rdd\
    .map(lambda x: [x['label'], [x['prediction_dt'], x['prediction_rf'], x['prediction_rf_cem']]])\
    .map(lambda x: pegaMaiorOcorrencia(x))\
    .filter(lambda x: x[0] == x[1])\
    .count()

"""Considerando que voce possui apenas duas classes: Não Alerta e Alerta. Calcule a acurácia de classificação na base de testes para os seguintes classificadores:

    Árvore de Decisão (from pyspark.ml.classification import DecisionTreeClassifier)
    Random Forest com 20 arvores (from pyspark.ml.classification import RandomForestClassifier, e numTrees=20 no construtor do RandomForestClassifier)
    Random Forest com 100 arvores (numTrees=100 no construtor do RandomForestClassifier)
        Dicas: Para isto, você irá precisar manipular o dataframe para alterar os valores da coluna label, por exemplo através de uma UDF


"""

# cria nova coluna baseada no valor do label (alerta - mantem valor, nao alerta - outro valor em comum)

dfTeste5 = dfTeste
dfTeste5.show(5)

def mudaLabel(x):
    try:
        if x == 1.0:
            return x
        else:
            return 0.0
    except:
        pass

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType

udfMudaLabel = udf(lambda x: mudaLabel(x), DoubleType())

dfTeste5New = dfTeste5.withColumn('label', udfMudaLabel(func.col('label')))

dfTeste5New.show(5)

dfTreino5 = dfTreino

dfTreino5New = dfTreino5.withColumn('label', udfMudaLabel(func.col('label')))
dfTreino5New.show(5)

#informação 5.A

dt = DecisionTreeClassifier(labelCol='label', featuresCol='features')
modeloDT5 = dt.fit(dfTreino5New)

predicaoDT5 = modeloDT5.transform(dfTeste5New)
predicaoDT5.select(func.col('prediction'), func.col('label'), func.col('id')).show(5)

MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
    .evaluate(predicaoDT5)



#informação 5.B

rf5 = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)
modeloRF5 = rf.fit(dfTreino5New)

predicaoRF5 = modeloRF5.transform(dfTeste5New)
predicaoRF5.select(func.col('prediction'), func.col('label'), func.col('id')).show(5)

MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
    .evaluate(predicaoRF5)



#informação 5.C

rf5Cem = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
modeloRF5Cem = rf.fit(dfTreino5New)

predicaoRF5Cem = modeloRF5Cem.transform(dfTeste5New)
predicaoRF5Cem.select(func.col('prediction'), func.col('label'), func.col('id')).show(5)

MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
    .evaluate(predicaoRF5Cem)



#informação 6 - Determine qual a quantidade de eventos Alerta (label = 3.0) classificados erroneamente 
# como outra classe (falso-negativo) para os classificadores do item 5

# para o Decision Tree

predicaoDT5.filter(func.col('Classe') == 'Alerta')\
    .filter(func.col('prediction')!= func.col('label'))\
    .count() / predicaoDT5.filter(func.col('Classe') == 'Alerta').count()

# RF com 20 trees

predicaoRF5.filter(func.col('Classe') == 'Alerta')\
    .filter(func.col('prediction')!= func.col('label'))\
    .count() / predicaoRF5.filter(func.col('Classe') == 'Alerta').count()

# RF com 100 trees

predicaoRF5Cem.filter(func.col('Classe') == 'Alerta')\
    .filter(func.col('prediction')!= func.col('label'))\
    .count() / predicaoRF5Cem.filter(func.col('Classe') == 'Alerta').count()



"""Faça busca de parametros dos classificadores desenvolvidos no item 5. Plote um gráfico relacionando a acurácia e os parametros otimizados

    Árvore de Decisão varie o parametro maxDepth de 1 a 20
    Random Forest varie o numTrees de 1 a 20


"""

#informação 7.A



from matplotlib import pyplot as plt

i=1
scores = []
for i in range(1,21):

    dt7 = DecisionTreeClassifier(labelCol='label', featuresCol='features', maxDepth = i)
    modeloDT7 = dt7.fit(dfTreino5New)   
    
    predicaoDT7 = modeloDT7.transform(dfTeste5New)
    predicaoDT7.select(func.col('prediction'), func.col('label'), func.col('id'))

    scores.append(MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
        .evaluate(predicaoDT7))
    
print(scores)

fig = plt.figure(figsize = (22,10))

plt.plot(range(1,21), scores)
plt.title('Decision Tree')
plt.xlabel('Max Depth')
plt.ylabel('Accuracy')
plt.show()



#informação 7.B

j=1
scoresRF = []
for j in range(1,21):

    rf7 = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=j)
    modeloRF7 = rf7.fit(dfTreino5New)

    predicaoRF7 = modeloRF7.transform(dfTeste5New)
    predicaoRF7.select(func.col('prediction'), func.col('label'), func.col('id'))
    
    scoresRF.append(MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction')\
        .evaluate(predicaoRF7))
    
print(scoresRF)

fig = plt.figure(figsize = (22,10))

plt.plot(range(1,21), scoresRF)
plt.title('Random Forest')
plt.xlabel('Num Trees')
plt.ylabel('Accuracy')
plt.show()

