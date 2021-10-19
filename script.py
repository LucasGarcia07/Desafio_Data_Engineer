import apache_beam as beam
import json

from utils.toStr import ToStr
from utils.splitRow import SplitRow
from utils.selectEstadosFields import SelectEstadosFields 
from utils.selectCovidFields import SelectCovidFields
from utils.generateCovidKey import GenerateCovidKey
from utils.generateEstadosKey import GenerateEstadosKey

covid_hist_path = ("data/HIST_PAINEL_COVIDBR_28set2020.csv")
estados_ibge_path = ("data/EstadosIBGE.csv")

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