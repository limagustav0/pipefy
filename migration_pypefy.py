import json
import requests
import pandas as pd
import os
import traceback


# token
pipefy_token = ""

url = "https://api.pipefy.com/graphql"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Bearer %s" % pipefy_token
}

def puxar_pipes():
  pipes = pd.read_excel('all_pipes.xlsx')

  lista = []
  pipes_erros = []
  for pipe in pipes['pipes']:
    dicionario = {}
    qr = f'''
    query MyQuery {{
      pipe(id: {pipe}) {{
        name
        id
      }}
    }}
    '''
    payload = json.dumps({'query': qr})
    responsePipe = requests.post(url, data=payload, headers=headers).json()
    
    try:
      dicionario['pipe'] = responsePipe['data']['pipe']['name']
      dicionario['id'] = responsePipe['data']['pipe']['id']
      lista.append(dicionario)
    except Exception as e:
       pipes_erros.append(pipe)
  df = pd.DataFrame(lista)

  df.to_excel('cards/nomes_pipes.xlsx', index=False)
  if len(pipes_erros) > 0:
     print('existem pipes com erros')
     print(pipes_erros)
  return lista 
     


def puxar_cards(pipe):
    id_cards = []
    queryPipe = f'''
    query MyQuery {{
      allCards(pipeId: {pipe}) {{
        edges {{
          node {{
            id
          }}
        }}
      }}
    }}
    '''
    payload = json.dumps({'query': queryPipe})
    responsePipe = requests.post(url, data=payload, headers=headers).json()
    for pipe in range(0, len(responsePipe['data']['allCards']['edges'])):
        id_cards.append(responsePipe['data']['allCards']['edges'][pipe]['node']['id'])
    
    return id_cards

def trademarketing():
    id_cards = extrair_dados1('302693359')
    dados_cards = []
    cards_erros = []
    nome_pipe = 'trademarketing'
    for card in range(0, len(id_cards)):
      queryCard = f'''
      query MyQuery {{
        card(id: {id_cards[card]}) {{
          attachments_count
          attachments {{
            url
          }}
          title
          url
          uuid
          summary_fields {{
            raw_value
            title
            type
            value
          }}
          fields {{
            value
            name
            native_value
          }}
          summary_attributes {{
            raw_value
            title
            type
            value
          }}
          summary {{
            title
            type
            value
          }}
          subtitles {{
            name
            native_value
            report_value
            field {{
              options
              label
            }}
          }}
        }}
      }}
      '''

      payload = json.dumps({'query': queryCard})
      response = requests.post(url, data=payload, headers=headers).json()
      dicionario = {}
      try:
        dicionario['id do pipe'] = id_cards[card]
        card = 0
        dicionario['nome vendedor'] = response['data']['card']['fields'][card]['native_value'] if response['data']['card']['fields'][card]['native_value'] else 'NA'
        dicionario['nome cliente'] = response['data']['card']['fields'][card + 1]['native_value'] if response['data']['card']['fields'][card + 1]['native_value'] else 'NA'
        dicionario['cliente uno'] = response['data']['card']['fields'][card + 2]['native_value'] if response['data']['card']['fields'][card + 2]['native_value'] else 'NA'
        dicionario['titulo da solicitação'] = response['data']['card']['fields'][card +3]['native_value'] if response['data']['card']['fields'][card +3]['native_value'] else 'NA'
        dicionario['departamento solicitante'] = response['data']['card']['fields'][card+4]['native_value'] if response['data']['card']['fields'][card+4]['native_value'] else 'NA'
        dicionario['regional solicitante'] = response['data']['card']['fields'][card+5]['native_value'] if response['data']['card']['fields'][card+5]['native_value'] else 'NA'
        dicionario['qual assunto se refere'] = response['data']['card']['fields'][card+6]['native_value'] if response['data']['card']['fields'][card+6]['native_value'] else 'NA'
        dicionario['descrição da solicitação'] = response['data']['card']['fields'][card+7]['native_value'] if response['data']['card']['fields'][card+7]['native_value'] else 'NA'
        dicionario['url'] = [response['data']['card']['attachments'][url]['url'] for url in range(0, response['data']['card']['attachments_count']) if response['data']['card']['attachments_count'] > 0] if [response['data']['card']['attachments'][url]['url'] for url in range(0, response['data']['card']['attachments_count']) if response['data']['card']['attachments_count'] > 0] else 'NA'
      except Exception as e:
        cards_erros.append(dicionario['id do pipe'])
         
      dados_cards.append(dicionario)
      df = pd.DataFrame(dados_cards)

      print(df)

      df.to_excel('cards/trademarketing.xlsx', index=False)

    return dados_cards, nome_pipe

def campanhasmkt():
    dados_cards = []
    cards_erros = []
    nome_pipe = 'campanhasmkt'

    id_cards = extrair_dados1('302693359')

    for card in range(0, len(id_cards)):
      queryCard = f'''
      query MyQuery {{
        card(id: {id_cards[card]}) {{
          attachments_count
          attachments {{
            url
          }}
          title
          url
          uuid
          summary_fields {{
            raw_value
            title
            type
            value
          }}
          fields {{
            value
            name
            native_value
          }}
          summary_attributes {{
            raw_value
            title
            type
            value
          }}
          summary {{
            title
            type
            value
          }}
          subtitles {{
            name
            native_value
            report_value
            field {{
              options
              label
            }}
          }}
        }}
      }}
      '''

      payload = json.dumps({'query': queryCard})
      response = requests.post(url, data=payload, headers=headers).json()
      dicionario = {}
      try:
        dicionario['id do pipe'] = id_cards[card]
        card = 0
        dicionario['titulo'] = response['data']['card']['title']
        dicionario['Selecione o perfil de cliente'] = response['data']['card']['fields'][0]['value']
        dicionario['Nome do Salão ou Loja'] = response['data']['card']['fields'][1]['value']
        dicionario['Código do cliente no UNO:'] = response['data']['card']['fields'][2]['value']
        dicionario['Nome do Vendedor:'] = response['data']['card']['fields'][3]['value']
        dicionario['Selecione o responsável BackOffice por sua região:'] = response['data']['card']['fields'][4]['value']
        dicionario['Códido do último pedido no UNO'] = response['data']['card']['fields'][5]['value']
        dicionario['Qual valor do último pedido? (Líquido)'] = response['data']['card']['fields'][6]['value']
        dicionario['Qual a objetivo dessa ação comercial?'] = response['data']['card']['fields'][7]['value']
        dicionario['Endereço completo do Salão/Loja'] = response['data']['card']['fields'][8]['value']
        dicionario['Configuração da ação'] = response['data']['card']['fields'][9]['value']
        dicionario['Tipo de Evento'] = response['data']['card']['fields'][10]['value']
        dicionario['Selecione o estado da sua região:'] = response['data']['card']['fields'][11]['value']
        dicionario['Degustação'] = response['data']['card']['fields'][12]['value']
        dicionario['Sugestão de data para o evento:'] = response['data']['card']['fields'][13]['value']
        dicionario['Qual o porte do evento?'] = response['data']['card']['fields'][14]['value']
        dicionario['O espaço do cliente comporta uma estrutura de no mínimo 1,20cm de largura?'] = response['data']['card']['fields'][15]['value']
        dicionario['Horário do Evento'] = response['data']['card']['fields'][16]['value']
        dicionario['Observação'] = response['data']['card']['fields'][17]['value']
        dicionario['Precisa que seja desenvolvido CONVITE DIGITAL?'] = response['data']['card']['fields'][18]['value']
      except Exception as e:
        cards_erros.append(dicionario['id do pipe'])
      dados_cards.append(dicionario)

      df = pd.DataFrame(dados_cards)

      print(df)

      df.to_excel('campanhasmk.xlsx', index=False)

    return dados_cards, nome_pipe

def audiovisual(lista_pipes):
    print('teste')
    

    lista_pipes = pd.DataFrame(lista_pipes)

    #print(lista_pipes['pipe'][0])

    for index, pipe in enumerate(lista_pipes['id']):
      cards_erros = []
      dados_cards = []

      pipename = lista_pipes['pipe'][index]

      lista_cards = puxar_cards(pipe)

      for index,card in enumerate(lista_cards):
        queryCard = f'''
        query MyQuery {{
          card(id: {card}) {{
            attachments_count
            attachments {{
              url
            }}
            title
            url
            uuid
            summary_fields {{
              raw_value
              title
              type
              value
            }}
            fields {{
              value
              name
              native_value
            }}
            summary_attributes {{
              raw_value
              title
              type
              value
            }}
            summary {{
              title
              type
              value
            }}
            subtitles {{
              name
              native_value
              report_value
              field {{
                options
                label
              }}
            }}
          }}
        }}
        '''

        payload = json.dumps({'query': queryCard})
        response = requests.post(url, data=payload, headers=headers).json()
        print(response)

        try:
          dicionario = {}
          for field in range(0,len(response['data']['card']['fields'])):
              dicionario['titulo'] =  response['data']['card']['title']
              dicionario['id do card'] = lista_cards[index]
              dicionario[response['data']['card']['fields'][field]['name']] = response['data']['card']['fields'][field]['value']
          dados_cards.append(dicionario)
        except Exception as e:
          cards_erros.append(lista_cards[card])
      try:
        df = pd.DataFrame(dados_cards)

        print(df)

        df.to_excel(f'cards/{pipename}.xlsx', index=False)
      except Exception as e:
         cards_erros.append(pipename)
        
    print('os pipes deram erro')
    print(pipename)


if __name__ == '__main__':
  id_pipes = puxar_pipes()
  audiovisual(id_pipes)
  #id_cards = extrair_dados1(id_pipes)
  #all_cards = audiovisual(id_cards)



