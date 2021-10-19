import apache_beam as beam

class ToStr(beam.DoFn):
    def process(self, element):
        '''
            Transforma uma tupla de listas em string formatada
    
            Args:
                element (tupple): tupla (coduf, [[regiao, uf, casosAcumulado, obitosAcumulado], [estado, governador]])
            Returns:
                string: regiao, estado, uf, governador, casosAcumulado, obitosAcumulado 
        '''
        if len(element[1][0]) > len(element[1][1]):
            lista = [element[1][0][0], element[1][0][1], element[1][1][1], element[1][1][0], element[1][0][2], element[1][0][3]]
        else:
            lista = [element[1][1][0], element[1][0][0], element[1][1][1], element[1][0][1], element[1][1][2], element[1][1][3]]
        return [','.join(lista)]
