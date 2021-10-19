import apache_beam as beam

class GenerateCovidKey(beam.DoFn):
    '''
    Gera uma tupla de listas a partir dos elementos selecionados
    
    Args:
        element (list): lista =>  [coduf, regiao, estado, casosAcumulado, obitosAcumulado]
    
    Returns: 
        tupple => (coduf, [regiao, estado, casosAcumulado, obitosAcumulado])
    
    '''
    def process(self, element):
        return [(element[0], [element[1], element[2], element[3], element[4]])]