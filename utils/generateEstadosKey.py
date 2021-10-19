import apache_beam as beam

class GenerateEstadosKey(beam.DoFn):
    '''
    Gera uma tupla de listas a partir dos elementos selecionados
    
    Args:
        element (list): lista =>  [coduf, governador, uf]
    
    Returns: 
        tupple => (coduf, [governador, uf])
    
    '''
    
    def process(self, element):
        return [(element[0], [element[1], element[2]])]