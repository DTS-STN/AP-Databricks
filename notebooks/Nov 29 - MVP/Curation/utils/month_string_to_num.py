# Databricks notebook source
@udf
def month_string_to_num(string):
    m = {'jan': '01',
         'feb': '02',
         'mar': '03',
         'apr': '04',
         'may': '05',
         'jun': '06',
         'jul': '07',
         'aug': '08',
         'sep': '09',
         'oct': '10',
         'nov': '11',
         'dec': '12'}
    
    s = string.strip()[:3].lower()

    try:
        result = m[s]
        return result
    except:
        raise ValueError('Not a month')
