import apache_beam as beam

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