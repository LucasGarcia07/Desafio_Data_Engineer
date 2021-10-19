import apache_beam as beam
import json

covid_hist_path = ("data/HIST_PAINEL_COVIDBR_28set2020.csv")
estados_ibge_path = ("data/EstadosIBGE.csv")

class ToStr(beam.DoFn):
    '''
    Transforma uma tupla de listas em string formatada
    
    Args:
        element (tupple): tupla entre coduf e lista de regiao, governador, uf, estado, casosAcumulado, obitosAcumulado
    Returns:
        strings
    '''
    def process(self, element):
        if len(element[1][0]) > len(element[1][1]):
            lista = [element[1][0][0], element[1][0][1], element[1][1][1], element[1][1][0], element[1][0][2], element[1][0][3]]
        else:
            lista = [element[1][1][0], element[1][0][0], element[1][1][1], element[1][0][1], element[1][1][2], element[1][1][3]]
        return [','.join(lista)]

class GenerateCovidKey(beam.DoFn):
    '''
    Gera uma tupla de listas a partir dos elementos selecionados
    
    Args:
        element (list): lista =>  [coduf, regiao, estado, casosAcumulado, obitosAcumulado]
    
    Returns: 
        tupple => (coduf, regiao, estado, casosAcumulado, obitosAcumulado )
    
    '''
    def process(self, element):
        return [(element[0], [element[1], element[2], element[3], element[4]])]

class GenerateEstadosKey(beam.DoFn):
    '''
    Gera uma tupla de listas a partir dos elementos selecionados
    
    Args:
        element (list): lista =>  [coduf, governador, uf]
    
    Returns: 
        tupple => (coduf, governador, uf)
    
    '''
    
    def process(self, element):
        return [(element[0], [element[1], element[2]])]

class SelectEstadosFields(beam.DoFn):
    '''
    Seleciona dentre todas as features extraídas, código, governador, uf
    
    Args:
        element(list): lista => [...] todas as features
    
    Returns:
        lista => [coduf, governador, uf]
    
    '''
    def process(self, element):
        return [element[1] + ',' + element[0] + ',' + element[3]]

class SelectCovidFields(beam.DoFn):
    '''
    Seleciona dentre todas as features extraídas, coduf, regiao, estado, casosAcumulado, obitosAcumulado
    
    Args:
        element(list): lista => [...] todas as features
    
    Returns:
        lista => [coduf, regiao, estado, casosAcumulado, obitosAcumulado]
    
    '''
    def process(self, element):
        return [element[3] + ',' + element[0] + ',' + element[1] + ',' + element[10] + ',' + element[12]]

class SplitRow(beam.DoFn):
    '''
    Transforma dados csv em listas
    
    Args:
        element(str): todas as features
    
    Returns:
        lista => [...] todas as features
    
    '''
    def process(self, element):
        new_element = element.split(';')
        yield new_element

pipe = beam.Pipeline()

covid_data = (pipe
     |"Extrair csv covid" >> beam.io.ReadFromText(covid_hist_path, skip_header_lines=True)
     |"Transformar covid csv em listas" >> beam.ParDo(SplitRow())
     |"Filtrar dados nacionais" >> beam.Filter(lambda x:x[3]!='76')
     |"Filtrar dados municipais" >> beam.Filter(lambda x:x[2]=='')
     |"Filtrar dados municipais resultantes" >> beam.Filter(lambda x:x[4]=='')
     |"Extrair apenas a ultima amostragem" >> beam.Filter(lambda x:x[7]=="28/09/2020")
     |"Selecionar apenas features relevantes do covid hist" >> beam.ParDo(SelectCovidFields())
     |"Retornar listas formatadas" >> beam.Map(lambda x:x.split(","))
     |"Gera uma lista de tuplas" >> beam.ParDo(GenerateCovidKey())
)

estados_data = (pipe
    |"Extrair csv estados" >> beam.io.ReadFromText(estados_ibge_path, skip_header_lines=True)
    |"Transformar estados csv em listas" >> beam.ParDo(SplitRow())
    |"Selecionar apenas features relevantes dos estados" >> beam.ParDo(SelectEstadosFields())
    |"Retornar listas dos estados formatadas" >> beam.Map(lambda x:x.split(","))
    |"Gera uma lista de tuplas dos estados" >> beam.ParDo(GenerateEstadosKey())
)

merged_data = ((covid_data,estados_data)
    |"Agrupa as duas Pcollections em uma nova Pcollection" >> beam.Flatten()
    |"Agrupa os dados por código de estados" >> beam.GroupByKey()
    |"Transforma uma tupla de listas em string" >> beam.ParDo(ToStr())
    |"Envia os dados para result_file.csv" >> beam.io.WriteToText("data/resultado",
                                                                  file_name_suffix='.csv',
                                                                  header='Regiao, Estado, UF, Governador, TotalCasos, TotalObitos')
)
pipe.run()
headers = ['Regiao', 'Estado', 'UF', 'Governador', 'TotalCasos', 'TotalObitos']

merged_json = (pipe
    |beam.io.ReadFromText("data/resultado-00000-of-00001.csv", skip_header_lines=True)
    |beam.Map(lambda line: dict(zip(headers, line.split(',') ) ) )
    |beam.Map(json.dumps)
    |beam.io.WriteToText("data/resultado", file_name_suffix='.json')
)

pipe.run()