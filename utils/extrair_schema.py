import boto3

def extrair_schema():
    print("Conectando ao Catálogo de Dados do AWS Glue...")
    glue = boto3.client('glue')
    database_name = 'db_energia_clima_puc'
    arquivo_saida = 'schema_silver_oficial.txt'

    try:
        # Busca todas as tabelas do banco de dados
        response = glue.get_tables(DatabaseName=database_name)
        tables = response.get('TableList',[])

        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            f.write(f"ESQUEMA DO BANCO DE DADOS: {database_name}\n")
            f.write("=" * 50 + "\n\n")

            for table in tables:
                table_name = table['Name']
                f.write(f"Tabela: {table_name}\n")
                f.write("-" * 40 + "\n")

                # Extrai colunas normais
                columns = table.get('StorageDescriptor', {}).get('Columns',[])
                for col in columns:
                    f.write(f"  {col['Name']} : {col['Type']}\n")

                # Extrai chaves de partição (se houver)
                partitions = table.get('PartitionKeys',[])
                if partitions:
                    f.write("\n  -- Colunas de Partição --\n")
                    for part in partitions:
                        f.write(f"  {part['Name']} : {part['Type']} (Particionado)\n")

                f.write("\n" + "=" * 50 + "\n\n")

        print(f"\nSUCESSO! O esquema foi salvo no arquivo: {arquivo_saida}")
        print("Você pode baixar este arquivo pelo menu 'Actions' > 'Download file' no CloudShell.")

    except Exception as e:
        print(f"Erro ao extrair o schema: {e}")

if __name__ == "__main__":
    extrair_schema()
