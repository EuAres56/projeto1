import pandas as pd
import requests
import gzip
import io
import itertools
import json
import os
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict

class main():
    def iniciar(self):
        urlBase = 'https://data.gharchive.org/2025-06-07-14.json.gz'
        arq = urlBase.split('/')[-1]
        print('\n')
        print('Iniciando captura de logs')
        #Dowload dos logs
        self.baixarLogs(urlBase)
        #Converte para dicionário separando por tipo de evento
        dicLogs = self.convertToDic(arq)
        #Converte dicionário em data frame
        dateFrame = self.convertToDF(dicLogs)
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
      
    def baixarLogs(self, url):
        response = requests.get(url, stream=True)

        # Nome do arquivo local
        arquivo_gz = url.split('/')[-1] 

        if response.status_code == 200:
            with open(arquivo_gz, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            print(f"Download concluído: {arquivo_gz}")
        else:
            print(f"Erro ao baixar: {response.status_code}")
            exit()

    def convertToDic(self, nameArq):
        userDic = {}

        #Define número de linhas por chunk dependo do numero de núcleos no cpu
        nCPU = os.cpu_count()
        size = 10000
        if nCPU > 4: size = 20000
        if nCPU > 8: size = 30000

        #Faz a leitura do arquivo json
        with gzip.open(nameArq, 'rt', encoding='utf-8') as strLogs:
            with ProcessPoolExecutor(max_workers=os.cpu_count()-1) as executor:
                futures = [executor.submit(processarLines, chunk) for chunk in self.chunkIterator(strLogs, size)]
                
                # Combina os resultados
                for future in as_completed(futures):
                    partialDic = future.result()
                    for user in partialDic:
                        if user not in userDic:
                            userDic[user] = partialDic[user]
                        else:
                            for part in partialDic[user]:
                                if part != 'login':
                                    if part not in userDic[user]: userDic[user][part] = partialDic[user][part]
                                    else:
                                        n1 = userDic[user][part]
                                        n2 = partialDic[user][part]
                                        userDic[user][part] = n1 + n2

        print('\nOs dados foram tratados e convertidos em um dicionário!')
        return userDic

    def convertToDF(self, dicLogs):
        #Cria data frame e substitui valores vazios por 0
        df = pd.DataFrame(dicLogs).fillna(0)
        print('O dicionário foi convertido em um data frame!')
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
            jsLog = json.loads(log)
            user = jsLog['actor']
            uId = user['id']
            event = jsLog['type']

            if uId not in partDic:
                partDic[uId] = {}
                partDic[uId]['login'] = user['login']

            if event not in partDic[uId]: partDic[uId][event] = 0
            partDic[uId][event] += 1
        except json.JSONDecodeError:
            errors += 1
            continue
    if errors > 0:
        print(f"[processarLines] {errors} linhas ignoradas por erro de JSON.")
    return partDic

if __name__ == '__main__':
    m = main()
    m.iniciar()