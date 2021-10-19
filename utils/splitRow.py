import apache_beam as beam

class SplitRow(beam.DoFn):
    def process(self, element):
        '''
            Transforma dados csv em listas
    
            Args:
                element(str): todas as features
            
            Returns:
                lista => [...] todas as features
        '''
        new_element = element.split(';')
        yield new_element