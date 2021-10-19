import apache_beam as beam

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