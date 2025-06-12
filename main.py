import pandas as pd
import gzip
import itertools
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
#para pontos que pedem maior leitura de codigo
import xlr8
#para leitura eficiente de json, melhor que a json padrão em termos de desempenho
import orjson
#para dowload mais eficiente que requests
import httpx
#para obter horarios e fazer calculos com horas
import time

class main():
    def iniciar(self):
        #Captura hora inicial, iniciando cronometro
        start_time = time.time()
        
        urlBase = 'https://data.gharchive.org/2025-06-07-14.json.gz'
        arq = urlBase.split('/')[-1]
        print('\n')
        print('Iniciando download dos logs')
        #Dowload dos logs
        self.baixarLogs(urlBase)
        # para o cronômetro
        download_time = time.time() - start_time
        print(f'Tempo de download de: {download_time}')

        #Captura hora inicial, iniciando cronometro
        start_time = time.time()

        #Converte para dicionário separando por tipo de evento
        dicLogs = self.convertToDic(arq)
        # para o cronômetro
        dict_time = time.time() - start_time
        print(f'Convertido em dict em: {dict_time}')

        #Captura hora inicial, iniciando cronometro
        start_time = time.time()

        #Converte dicionário em data frame
        dateFrame = self.convertToDF(dicLogs)
        # para o cronômetro
        dtFrame_time = time.time() - start_time
        print(f'DataFrame estruturado em: {dtFrame_time}')

        #Captura hora inicial, iniciando cronometro
        start_time = time.time()

        #Calcula o total de usuários
        totalUser = dateFrame.shape[1]
        #Calcula o total de eventos realizado por cada usuário
        totalEvUser = self.calcularTotalEventos(dateFrame)
        #Calcula o total de eventos realizado por cada usuário
        totalEventos = self.calcularEventos(totalEvUser)
        
        print('\nProcessamento finalizado!')
        print(f'\nO total de usuários é {totalUser}')
        print(f'\nO total de eventos é {totalEventos}')
        print('\n')
        print('Os 10 usuários que mais realizaram eventos foram:')

        #Reorganiza do maior para o maior baseado segundo valor
        tEvUser = sorted(totalEvUser, key=lambda x: x[1], reverse=True)

        #Imprime no console os dez usúarios com mais eventos registrados
        c = 1
        for user, ev in tEvUser:
            print(f'{user} com {ev} eventos')
            c +=1
            if c >10: break
        
        #deleta arquivo
        os.remove(arq)
        
        #processamentos rapidos
        spProccess_time = time.time() - start_time
        print(f'Processos adicionais em: {spProccess_time}')

        # tempo total
        procces_time = download_time + dict_time + dtFrame_time + spProccess_time
        print(f'Tempo de processamento de: {procces_time-download_time}')
        print(f'Tempo total de execução de: {procces_time}')
      
    def baixarLogs(self, url):
        response = httpx.Client().get(url)

        # Nome do arquivo local
        arquivo_gz = url.split('/')[-1] 

        if response.status_code == 200:
            with open(arquivo_gz, 'wb') as f:
                f.write(response.content)
        else:
            print(f"Erro ao baixar: {response.status_code}")
            exit()

    def convertToDic(self, nameArq):
        #Define número de linhas por chunk dependo do numero de núcleos no cpu
        nCPU = os.cpu_count()
        size = 15000
        if nCPU > 4: size = 30000
        if nCPU > 8: size = 50000

        #Faz a leitura do arquivo json
        with gzip.open(nameArq, 'rb') as strLogs:
            with ProcessPoolExecutor(max_workers=os.cpu_count()-1) as executor:
                futures = [executor.submit(processarLines, chunk) for chunk in self.chunkIterator(strLogs, size)]
                
                # Combina os resultados
                listParcials = []
                for future in as_completed(futures):
                    listParcials.append(future.result())

        userDic = xlr8.combinaUsers(listParcials)
        return userDic

    def convertToDF(self, dicLogs):
        #Cria data frame e substitui valores vazios por 0
        df = pd.DataFrame(dicLogs).fillna(0)
        return df

    def calcularTotalEventos(self, df):
        #Faz a soma dos valores nas linhas
        totalUserEv = df.iloc[1:].sum(axis=0)
        #Retorna col 0 (login), resultado das somas
        listEvUser = list(zip(df.iloc[0], totalUserEv))
        return listEvUser

    def calcularEventos(self, lista):
        total = sum(ev for login, ev in lista)
        return total

    def chunkIterator(self, iterable, size):
        it = iter(iterable)
        while True:
            chunk = list(itertools.islice(it, size))
            if not chunk:
                break
            yield chunk

def processarLines(strLogs):
    partDic = {}
    errors = 0
    for log in strLogs:
        try:
            if isinstance(log, str):
                jsLog = orjson.loads(log.encode())
            else:
                jsLog = orjson.loads(log)
            user = jsLog['actor']
            uId = user['id']
            event = jsLog['type']

            if uId not in partDic:
                partDic[uId] = {}
                partDic[uId]['login'] = user['login']

            if event not in partDic[uId]: partDic[uId][event] = 0
            partDic[uId][event] += 1
        except orjson.JSONDecodeError:
            errors += 1
            continue
    if errors > 0:
        print(f"[processarLines] {errors} linhas ignoradas por erro de JSON.")
    return partDic

if __name__ == '__main__':
    m = main()
    m.iniciar()