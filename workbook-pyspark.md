
# Biblioteca de codigos em PySpark - databricksgit status


## Exemplos de codigos utilizados para ler/ver/imprimir os dados 

### definindo as tabela em dataframe Leitura de dado parquet
```
        df = spark.read.parquet("/mnt/cleaned/varejo/cantu/fator_distribuicao_cnae_2021")
```

### definindo as tabela em dataframe Leitura de dado csv
``` 
        df_df = spark.read.csv("/mnt/raw/ftp/educacao/censo_escolar/microdados_ed_basica/microdados_ed_basica_2016.csv", header=True, sep=";")
```

### Lendo 1 arquivo JSON 
 ``` 
        dataf = spark.read.json("/databricks-datasets/structuredstreaming/events/file-1.json")
```

### leitura de arquivos particionados em parquet
```        
        df_saeb_escolas = spark.read \
            .option("basePath", "/mnt/curated/educacao/dash_educacao/saeb") \
            .parquet("/mnt/curated/educacao/dash_educacao/saeb/ano=*/granularidade=escolas") #FFF;
```

### Lendo todos os arquivos JSON
            dataf3 = spark.read.json("/databricks-datasets/structuredstreaming/events/*.json")

## Exemplo de codigo para salvar os dados 

### Salvar o DataFrame final em um único arquivo CSV
    df.repartition(1).write.csv(output_path, mode='overwrite', header=True, sep=';', encoding='ISO-8859-1')


### Salvando dados no delta 
    df_ft_ideb_saeb_escolas_matri.write.format('delta') \
        .mode('overwrite') \
        .option('overwriteSchema', 'true') \
        .option('delta.columnMapping.mode', 'name') \
        .option('delta.minReaderVersion', '2') \
        .option('delta.minWriterVersion', '5') \
        .saveAsTable("educacao.dash_educacao_ft_ideb_saeb_escolas")

### Gravando o arquivo parquet
    datafparquet.write.parquet("/FileStore/tables/parquet/pessoal.parquet")

### Permite uma atualização do arquivo parquet  Subscrevendo o arquivo parquet

    datafparquet.write.mode('overwrite').parquet('/FileStore/table/parquet/pessoal.parquet')

###  salvando arquivos particionados 
    
    df_organizado.write.partitionBy("ano", "granularidade","etapa_ensino").mode("append").parquet("/mnt/curated/educacao/dash_educacao/ideb")

## baixar os dados direto de um site e salvar no blob

    url = "http://sped.rfb.gov.br/arquivo/download/5630"
    filename = "NFe_PorCNAE_Estabelecimento_2019.ods"
    target_dir = "/dbfs/mnt/raw/ftp/sped/NFe_PorCNAE_Estabelecimento_2019.ods"

    response = requests.get(url)

    if response.status_code == 200:
        with open(os.path.join(target_dir), "wb") as f:
                f.write(response.content)
        print(f"Arquivo {filename} baixado com sucesso.")
    else:
        print(f"Não foi possível baixar o arquivo. Código de status: {response.status_code}")

## Listar os arquivos zip no diretório
        zip_files = [f for f in os.listdir(zip_directory) if f.endswith(".zip")]

        # Iterar sobre os arquivos zip
        for zip_file in zip_files:
            # Caminho completo do arquivo zip
            zip_path = os.path.join(zip_directory, zip_file)

            try:
                # Extrair o conteúdo do arquivo zip com senha
                with pyzipper.AESZipFile(zip_path) as zf:
                    zf.extractall(path=zip_directory)
            except (RuntimeError, pyzipper.BadZipFile):
                print(f"O arquivo zip '{zip_file}' está protegido por senha e não pode ser extraído.")
                
                
                # Define o diretório raiz
        root_dir = "/mnt/raw/ftp/ans/dados_de_beneficiarios_por_operadora"

## Lê os arquivos CSV em subdiretórios e carrega em um DataFrame
    
        df_beneficiarios_por_operadora = spark.read.format("csv") \
                    .option("header", True) \
                    .option("delimiter", ";") \
                    .option("inferSchema", True) \
                    .load(f"{root_dir}/*.csv") 
        # Mostra o DataFrame
        df_beneficiarios_por_operadora.count()

## codigos de tratamento dos dados 

### Realize o join entre os estudos
        df_consolidado_estudo = df_consolidado_estudo_cnpj_8_digitos.join(df_select_cnpj_8_digitos,  df_filtro_zeta_cnpj_8_digitos.cnpj_8_digitos_estudo == df_select_cnpj_8_digitos.cnpj_8_digitos_estudo, 'left')

### contagem do numero de linhas 
        num_linhas_df_fator_distribuicao_cnae_2021 = df_fator_distribuicao_cnae_2021.count()

### Imprimir o número de linhas
        print(f"O número total de linhas fator_distribuicao_cnae_2021 no DataFrame é: {num_linhas_df_fator_distribuicao_cnae_2021}")

### definindo o tipo do dado da coluna
        df_calculo1 = df_calculo1.withColumn("ano", df_calculo1.ano.cast("string"))

### fazendo um group by
        df_teste123 = df_calculo1.groupBy('ano', 'co_sub_classe', 'valor_ponderado_receita_cnae' ) \
        .agg(F.sum('mercado_top_down'),F.sum('fator_distribuicao_mercado') )

### codigo para identificar a quantidade de tabelas presente em uma pasta, mostra a quantidade de linhas e tudo mais
        display(dbutils.fs.ls("/mnt/raw/ftp/educacao/ideb/2019/"))

### Lista de arquivos Json que estão armazenados no DBFS
        %fs ls /databricks-datasets/structured-streaming/events/

### Exibindo um arquivo Json com as informações
        %fs head /databricks-datasets/structured-streaming/events/file1.json

### Criação de uma tabela para executar SQL
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json 
        OPTIONS" + 
        " (path '/FileStore/tables/JSON/eventos.json')")
        spark.sql("select action from view_evento").show()

### Realizando uma consulta SQL Criando uma consulta em SQL 
        Obs. essa função é menos performatica
        datafleitura.createOrReplaceTempView("Tabela_Parquet")
        ResultSQL = spark.sql("select * from Tabela_Parquet where salario >= 6000 ")
        ResultSQL.show()

### Exibindo os dados do parquet cuja a nacionalidade é brasileira Lendo o aquivo participonado do parquet
        datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira")
        datafnacional.show(truncate=False)

### Realizando uma pesquisa via SQL no arquivo parquet particionado Consultando diretamente o arquivo parquet particionado via 
        SQL
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW Cidadao USING parquet OPTIONS (path \"/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira\")")
        spark.sql("SELECT * FROM Cidadao where Ultimo_nome='Oliveira'").show()

### Selecionar valores distintos da coluna 'etapa_ensino'
        valores_distintos_etapa_ensino = ideb_municipios.select('etapa_ensino').distinct()
