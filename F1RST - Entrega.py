#!/usr/bin/env python
# coding: utf-8

# In[69]:


import json
import pandas as pd

import csv


# In[40]:


with open("J://Desenvolvimento//F1rst//desafio//teste_python//ecommerce.json", 'r') as f:
    data = json.load(f)


# In[459]:


df_source = pd.json_normalize(data["hits"]["hits"])


# In[188]:


def function_filter_rules(df_source_filter, var_filter_nm : str):
    
    if var_filter_nm == '1':
        id_rule = '1'
        nm_rule = 'men_weekend'
        var_week   = ['Saturday','Sunday']
        var_gender = ['MALE']
        
        df_return = df_source_filter.query("`_source.day_of_week` in (@var_week) & `_source.customer_gender` in (@var_gender) ")[["_source.order_date", "_source.order_id", "_source.customer_gender", "_source.day_of_week"]]
        df_return["id_rule"] = id_rule
        df_return["nm_rule"] = nm_rule
    
    elif var_filter_nm == '2':
        id_rule = '2'
        nm_rule = 'men_week'
        var_week   = ['Saturday','Sunday']
        var_gender = ['MALE']
        
        df_return = df_source_filter.query("`_source.day_of_week` not in (@var_week) & `_source.customer_gender` in (@var_gender) ")[["_source.order_date", "_source.order_id", "_source.customer_gender", "_source.day_of_week"]]
        df_return["id_rule"] = id_rule
        df_return["nm_rule"] = nm_rule
        
   
    elif var_filter_nm == '3':
        id_rule = '3'
        nm_rule = 'women_weekend'
        var_week   = ['Saturday','Sunday']
        var_gender = ['FEMALE']
        
        df_return = df_source_filter.query("`_source.day_of_week` in (@var_week) & `_source.customer_gender` in (@var_gender) ")[["_source.order_date", "_source.order_id", "_source.customer_gender", "_source.day_of_week"]]
        df_return["id_rule"] = id_rule
        df_return["nm_rule"] = nm_rule
        
   
    elif var_filter_nm == '4':
        id_rule = '4'
        nm_rule = 'women_week'
        var_week   = ['Saturday','Sunday']
        var_gender = ['FEMALE']
        
        df_return = df_source_filter.query("`_source.day_of_week` not in (@var_week) & `_source.customer_gender` in (@var_gender) ")[["_source.order_date", "_source.order_id", "_source.customer_gender", "_source.day_of_week"]]
        df_return["id_rule"] = id_rule
        df_return["nm_rule"] = nm_rule
        
    return df_return


# In[528]:


df_temp = pd.concat([
    function_filter_rules(df_source, '1')
    ,function_filter_rules(df_source, '2')
    ,function_filter_rules(df_source, '3')
    ,function_filter_rules(df_source, '4')]
    ,ignore_index=True
)


# # Filtro dinamico
# 
# ### Query para o filtro dinamico dos dados

# In[464]:


def function_filter_auto(df_source, var_filter):
    
    df_filter = df_source.query(var_filter)
    
    return df_filter


# In[525]:


df_temp = pd.DataFrame()
rows = []

with open("J://Desenvolvimento//F1rst//desafio//teste_python//rules.txt", 'r') as file:
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        
        var_id = row[0]
        var_rule_name = row[1]
        var_rule_filter = row[2] + "," + row[3]
        
        var_rule_ajust = str(var_rule_filter).replace("day_of_week", "_source.day_of_week").replace("customer_gender", "_source.customer_gender")
        
        df = function_filter_auto(df_source, var_rule_ajust)
        df = df[["_source.order_date","_source.order_id"]]
        df[["id_rule", "nm_rule"]] = var_id, var_rule_name
        
        df_temp = pd.concat([df, df_temp], ignore_index=True )
        


# In[527]:





# # Save Result
# 
# ### Salvar a saida de cada code

# In[ ]:


df_temp.to_csv("J://Desenvolvimento//F1rst//desafio//ecommerce_saida.csv", sep=';', encoding='utf-8')

