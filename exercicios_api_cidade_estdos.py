# Databricks notebook source
import pandas as pd
import requests, json

url = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos"

response = requests.get(url)

if response.status_code ==200:
    data =json.loads(response.text)
    print(response.text)
else:
    print(f"Error: {response.status}")
    
data=json.loads(response.text)
###Conexão com API do IBGE, obtendo as informações de localidade do Rio de Janeiro e São Paulo

# COMMAND ----------

json_string = json.dumps(data)

# COMMAND ----------

json_string


# COMMAND ----------

display(dbutils.fs.ls('/FileStore/dados/'))

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/dados/cidades')

# COMMAND ----------

dbutils.fs.put('/FileStore/dados/cidades/dataset_cidades.json', json_string, overwrite=True)



# COMMAND ----------

display(dbutils.fs.ls('/FileStore/dados/cidades'))

# COMMAND ----------

df_cidades = spark.read.json('/FileStore/dados/cidades/dataset_cidades.json')

# COMMAND ----------

df_cidades.show()

# COMMAND ----------

  display(df_cidades.select('municipio.*', 'municipio.microrregiao.mesorregiao.UF.sigla'))

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


display(df_cidades.select('municipio.regiao-imediata.regiao-intermediaria.UF.nome').distinct())

# COMMAND ----------

df_estados1=df_cidades.withColumn('estados',col('municipio.regiao-imediata.regiao-intermediaria.UF.nome'))\
                    .withColumn('UF', col('municipio.microrregiao.mesorregiao.UF.sigla'))

# COMMAND ----------

df_estados1.show()

# COMMAND ----------

display(df_estados1)

# COMMAND ----------

df_estados2=df_estados1.drop('nome')

# COMMAND ----------

df_estados3=df_estados2.drop('municipio')

# COMMAND ----------

display(df_estados3)

# COMMAND ----------

display(df_cidades)

# COMMAND ----------

df_cidades=df_cidades.drop('municipio')

# COMMAND ----------

display(df_cidades)

# COMMAND ----------

df_cidades_estados=df_cidades.join(df_estados3, df_cidades.id==df_estados3.id)

# COMMAND ----------

df_cidades_estados.show()

# COMMAND ----------

df_cidades_estados2=df_cidades_estados.drop('id')

# COMMAND ----------

display(df_cidades_estados2)

# COMMAND ----------

display(df_cidades_estados2.groupBy('estados').count().orderBy('count', ascending=False))

# COMMAND ----------

path='/FileStore/dados/cidades_estados/parquet/'

df_cidades_estados2.write.mode('overwrite').option('compression','gzip').format('parquet').save('/FileStore/dados/cidades_estados/parquet/')

# COMMAND ----------

dbutils.fs.ls('path')

# COMMAND ----------

path= '/FileStore/dados/estados/'
df_estados.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path)

# COMMAND ----------

df = spark.read.format('parquet') \
    .load('/FileStore/dados/cidades_estados/parquet/', compression='gzip')




# COMMAND ----------

import pandas as pd

df_pd =df.toPandas()

# COMMAND ----------

df_pd

# COMMAND ----------

df_pd[df_pd['UF']=='MG']

# COMMAND ----------

df_cidades_estados= spark.read.parquet('/FileStore/dados/cidades_estados/parquet/')

# COMMAND ----------

display(df_cidades_estados)

# COMMAND ----------

df_cidades_estados.createOrReplaceTempView('df_tmp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  count(0),  estados, rank() over (partition by count(0) order by estados desc) as teste
# MAGIC  from df_tmp_view 
# MAGIC group by estados
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/dados/estados'))

# COMMAND ----------

resultado= spark.sql("""
          select * 
          from df_tmp_view where uf='SP'
          """)

# COMMAND ----------

resultado.createOrReplaceTempView('nova_tabela')

# COMMAND ----------

spark.sql('''
            select count(0), estados from nova_tabela group by estados 

          ''').show()



# COMMAND ----------


