Biblioteca de codigos em PySpark - databricksgit status


#Realize o join entre os estudos
df_consolidado_estudo = df_consolidado_estudo_cnpj_8_digitos.join(df_select_cnpj_8_digitos,  df_filtro_zeta_cnpj_8_digitos.cnpj_8_digitos_estudo == df_select_cnpj_8_digitos.cnpj_8_digitos_estudo, 'left')


#. Salvar os dados em csv em outro lugar
output_path = "/mnt/cleaned/varejo/cantu/df_select_cnpj_8_digitos.csv"  # Substitua pelo caminho de saída desejado

#Salvar o DataFrame final em um único arquivo CSV
df_select_cnpj_8_digitos.repartition(1).write.csv(output_path, mode='overwrite', header=True, sep=';', encoding='ISO-8859-1')


#definindo as tabela em dataframe Leitura de dado parquet
df_fator_distribuicao_cnae_2021 = spark.read.parquet("/mnt/cleaned/varejo/cantu/fator_distribuicao_cnae_2021")
#definindo as tabela em dataframe Leitura de dado csv
df_raw2016 = spark.read.csv("/mnt/raw/ftp/educacao/censo_escolar/microdados_ed_basica/microdados_ed_basica_2016.csv", header=True, sep=";"

##contagem do numero de linhas 
num_linhas_df_fator_distribuicao_cnae_2021 = df_fator_distribuicao_cnae_2021.count()

## Imprimir o número de linhas
print(f"O número total de linhas fator_distribuicao_cnae_2021 no DataFrame é: {num_linhas_df_fator_distribuicao_cnae_2021}")

#definindo o tipo do dado da coluna
df_calculo1 = df_calculo1.withColumn("ano", df_calculo1.ano.cast("string"))

#fazendo um group by
df_teste123 = df_calculo1.groupBy('ano', 'co_sub_classe', 'valor_ponderado_receita_cnae' ) \
    .agg(F.sum('mercado_top_down'),F.sum('fator_distribuicao_mercado') )


#codigo para identificar a quantidade de tabelas presente em uma pasta, mostra a quantidade de linhas e tudo mais
display(dbutils.fs.ls("/mnt/raw/ftp/educacao/ideb/2019/"))
faixa_faturamento
faixa_funcionario

#Lista de arquivos Json que estão armazenados no DBFS
%fs ls /databricks-datasets/structured-streaming/events/
#Exibindo um arquivo Json com as informações
%fs head /databricks-datasets/structured-streaming/events/file1.json
#Carregando 1 arquivo Json para o dataframe
%python
# Lendo 1 arquivo JSON 
dataf = spark.read.json("/databricks-datasets/structuredstreaming/events/file-1.json")
dataf.printSchema()
dataf.show()
#Carregando 2 arquivos Json para o dataframe
%python
#Lendo 2 arquivos JSON
dataf2 = spark.read.json(['/databricks-datasets/structuredstreaming/events/file-1.json','/databricks-datasets/structuredstreaming/events/file-2.json'])
dataf2.show() 
#Carregando TODOS os arquivos Json para o dataframe
%python
#Lendo todos os arquivos JSON
dataf3 = spark.read.json("/databricks-datasets/structuredstreaming/events/*.json")
dataf3.show()

#Unificando todos os arquivos que foram guardados no dataframe dataf3 para um 
novo arquivo JSON
%python
##Gravação dos dados que estão no dataframe para JSON em um único 

arquivo
dataf3.write.json("/FileStore/tables/JSON/eventos.json")

#Criação de uma tabela para executar SQL
%python
spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json 
OPTIONS" + 
 " (path '/FileStore/tables/JSON/eventos.json')")
spark.sql("select action from view_evento").show()


Criação de um dataframe com dados
#criando um dataframe com dados fixos
dados =[("Grimaldo
","Oliveira","Brasileira","Professor","M",3000),
 ("Ana ","Santos","Portuguesa","Atriz","F",4000),
 
("Roberto","Carlos","Brasileira","Analista","M",4000),
 ("Maria 
","Santanna","Italiana","Dentista","F",6000),
 
("Jeane","Andrade","Portuguesa","Medica","F",7000)]
colunas=["Primeiro_Nome","Ultimo_nome","Nacionalidade","Trabal
ho","Genero","Salario"]
datafparquet=spark.createDataFrame(dados,colunas)
datafparquet.show()

#Gravando o arquivo parquet
#criando o arquivo parquet
datafparquet.write.parquet("/FileStore/tables/parquet/pessoal.
parquet")

#Subscrevendo o arquivo parquet
#Permite uma atualização do arquivo parquet
datafparquet.write.mode('overwrite').parquet('/FileStore/table
s/parquet/pessoal.parquet')

#Lendo o arquivo parquet e guardando em um dataframe
#Realizando uma atualização do arquivo parquet
datafleitura= 
spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet"
)
datafleitura.show()

#Realizando uma consulta SQL Criando uma consulta em SQL 
    #essa função é menos performatica
datafleitura.createOrReplaceTempView("Tabela_Parquet")
ResultSQL = spark.sql("select * from Tabela_Parquet where salario >= 6000 ")
ResultSQL.show()

#Particionando os dados do arquivo parquet em grupos #Particionando os dados em um arquivo parquet
datafparquet.write.partitionBy("Nacionalidade","salario").mode
("overwrite").parquet("/FileStore/tables/parquet/pessoal.parqu
et")

#Exibindo os dados do parquet cuja a nacionalidade é brasileira #Lendo o aquivo participonado do parquet
datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira")
datafnacional.show(truncate=False)

#Realizando uma pesquisa via SQL no arquivo parquet particionado #Consultando diretamente o arquivo parquet particionado via 
SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW Cidadao USING parquet OPTIONS (path \"/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira\")")
spark.sql("SELECT * FROM Cidadao where Ultimo_nome='Oliveira'").show()

# Selecionar valores distintos da coluna 'etapa_ensino'
valores_distintos_etapa_ensino = ideb_municipios.select('etapa_ensino').distinct()

# Mostrar o resultado
valores_distintos_etapa_ensino.show()
