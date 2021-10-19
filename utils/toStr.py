import apache_beam as beam

class ToStr(beam.DoFn):
    def process(self, element):
        '''
            Transforma uma tupla de listas em string formatada
    
            Args:
                element (tupple): tupla (coduf, [[lista de regiao, governador, uf, estado, casosAcumulado, obitosAcumulado
            Returns:
                strings
        '''
        if len(element[1][0]) > len(element[1][1]):
            lista = [element[1][0][0], element[1][0][1], element[1][1][1], element[1][1][0], element[1][0][2], element[1][0][3]]
        else:
            lista = [element[1][1][0], element[1][0][0], element[1][1][1], element[1][0][1], element[1][1][2], element[1][1][3]]
        return [','.join(lista)]
