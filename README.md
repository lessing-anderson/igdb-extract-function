# Introduction 
Projeto python para criar Azure Function que extrai dados da API do IGDB.

# Param and Run Local
1.	Deverá ser instalado extensões "Azure Functions" e "Azurite" no vscode.
2.	Deverá ser instalado "Azure Functions Core Tools".
2.  Deverá ser criado as seguintes Secrets no Azure Key Vault: "valueClientIdTwitch" e "secretTwitch".
3.  Parâmetro "key_vault_url" que contem a URL do AKV deverá ser atualizados se necessário no arquivo "function_app.py". 
4.  Deverá ser dado permissão na storage account com a Role "Storage Blob Data Contributor" e no container com a permissão de Leitura/Escrita/Execução para o usuário usado para logar no "az login". Para verificar usuário logado usar comando "az account show".
5.  Para prosseguir com a incialização da Azure Function deverá ser iniciado a extensão "Azurite", para isso aperte F1 e digite "Azurite: Start".
5.	Para rodar a função localmente deverá ser Startado o Debugger (F5).
6.	Após o start acontecer deverá ir na extensão Azure e no workspace Local clicar com direito sobre a Function e clicar no comando "Execute Function Now"
7.  Exemplo de parametros que deverão ser passados no body:

{
"endpoint": "games",
"extract_type": "delta",
"delta_days": 1,
"account_name": "dlhsheepdev",
"storage_path": "IGDB/"
}

# Deploy
1.	Deverá ser criado "Function App" no Azure Portal.
2.  Deverá ser dado permissão na storage account com a Role "Storage Blob Data Contributor" e no container com a permissão de Leitura/Escrita/Execução para a Managed Identity do Function App.
3.  Após isso, na extensão Azure no vscode, clicar com direito sobre a Function App e selecionar a opção "Deploy to Function App".
