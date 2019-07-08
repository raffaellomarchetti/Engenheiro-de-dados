import json
import pandas as pd
#variavel para armazenar o caminho do entrada
input_path = 'input/page-views.json'
#variavel para armazenar o caminho de saida 
output_path = 'output/abandoned-carts.json'
a=[]
#Cria uma lista com os dados presentes no arquivo de entrada, cada item da lista é um json
with open(input_path, 'r') as entrada:
    for line in entrada:
        a.append(json.loads(line))
#Cria um dataframe baseado na lista de json's
df=pd.DataFrame(a)
#Ordena as colunas do df de acordo com a ordem de colunas presente no arquivo de entrada
df=df[['timestamp','customer','page','product']]
#Gera um novo dataframe com json's que efeturam uma compra
df_compra = df.query("page == 'checkout'").groupby(['customer','page','product','timestamp'],as_index=False).head()
#Gera um dataframe onde ficará armazenado os abandonos de carrinhos, data frame criado pelo join de todas as ações com o dataframe de dados de 'comprados'
df_abandono = df.merge(df_compra,on='customer',how='outer', indicator=True)
df_abandono = df_abandono[df_abandono['_merge']=='left_only']
#Renomeia as colunas do dataframe de abandono e ordena de acordo com a ordem de entrada
df_abandono = df_abandono[['timestamp_x','customer','page_x','product_x']].rename(columns={'page_x':'page','product_x':'product','timestamp_x':'timestamp'})
df_abandono = df_abandono[df_abandono['timestamp']==df_abandono['timestamp'].max()]
#Gera o arquivo de saida com os dados do abandono do carrinho
df_abandono.to_json(output_path,orient='records',lines=True)
