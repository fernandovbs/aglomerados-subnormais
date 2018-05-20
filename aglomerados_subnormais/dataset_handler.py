import json
import click

from aglomerados_subnormais import app, mongo

geocodigos = []
areas_ponderacao = {}

def init_db():
    with app.open_resource('static/aglomeradossubnormais.json') as f:
        aglomerados_json =  json.load(f)
        for feature in aglomerados_json['features']:
            geocodigos.append(feature['properties']['GEOCÓDIGO'])

    with app.open_resource('static/areasdeponderacao.txt') as f:
        for line in f:
            key, val = line.decode().split(',')

            if val.strip() in geocodigos:
                collection_key = key+'_1'
                collection_key2 = key+'_2'
                if collection_key in areas_ponderacao:
                    areas_ponderacao[collection_key]['geocodigo'].append(val.strip())
                    areas_ponderacao[collection_key2]['geocodigo'].append(val.strip())
                else:
                    areas_ponderacao[collection_key] = {}
                    areas_ponderacao[collection_key]['microdados'] = {'domicilios': [], 'pessoas': [], 'mortalidade': []}
                    areas_ponderacao[collection_key]['geocodigo'] = [val.strip()]
                    areas_ponderacao[collection_key]['_id'] = collection_key
                    areas_ponderacao[collection_key2] = {}
                    areas_ponderacao[collection_key2]['microdados'] = {'domicilios': [], 'pessoas': [], 'mortalidade': []}
                    areas_ponderacao[collection_key2]['geocodigo'] = [val.strip()]
                    areas_ponderacao[collection_key2]['_id'] = collection_key2                    

    create_database()

def create_database():
    areas_db = mongo.db.areas_ponderacao
    areas_db.delete_many({})
    areas_ponderacao_list = [y for (x, y) in areas_ponderacao.items()]

    click.echo('Inserindo dados das áreas de ponderação...')
    areas_db.insert(areas_ponderacao_list)
    
    insert_microdados()

def insert_microdados():
    click.echo('Inserindo microdados...')
    insert_domicilios()
    insert_pessoas()
    insert_mortalidade()
    click.echo('Concluído!')

def insert_domicilios():
    layout = [{'uf':(0,2)}, {'municipio':(2,7)}, {'area_ponderacao':(7,20)}, {'controle':(20,28)}, {'peso':(28,44)}, {'regiao':(44,45)}, {'mesoregiao':(45,47)}, 
        {'microregiao':(47,50)}, {'regiao_metropolitana':(50,52)}, {'situacao_domicilio':(52,53)}, {'especie_unidade':(53,55)},
        {'tipo_especie':(55,57)}, {'condicao_ocupacao':(57,58)}, {'valor_aluguel':(58,64)}, {'aluguel_em_salarios':(64,73)}, {'material_paredes':(73,74)}, {'numero_comodos':(74,76)},
        {'densidade_comodo':(76,79)}, {'numero_dormitorios':(79,81)}, {'densidade_dormitorio':(81,84)}, {'numero_banheiros':(84,85)}, {'existencia_sanitarios':(85,86)},
        {'tipo_esgotamento':(86,87)}, {'abastecimento_agua':(87,89)}, {'canalizacao_agua':(89,90)}, {'lixo_destino':(90,91)}, {'existencia_energia_eletrica':(91,92)},
        {'existencia_medidor_energia':(92,93)}, {'existencia_radio':(93,94)}, {'existencia_tv':(94,95)}, {'existencia_lavadora':(95,96)}, {'existencia_geladeira':(96,97)}, 
        {'existencia_celular':(97,98)}, {'existencia_telefone':(98,99)}, {'existencia_computador':(99,100)}, {'existencia_acesso_internet':(100,101)}, {'motocicleta': (101,102)}, 
        {'automovel':(102,103)}, {'pessoa_outro_pais':(103,104)}, {'total_pessoas':(104,106)}, {'responsabilidade_domicilio':(106,107)}, {'existencia_mortalidade':(107,108)},
        {'rendimento_domiciliar':(108,115)}, {'rendimento_salarios':(115,125)}, {'rendimento_per_capita':(125,133)}, {'rendimento_per_capita_salarios':(133,142)}, 
        {'especie_unidade_domestica':(142,143)}, {'adequacao_moradia':(143,144)}, {'situacao_setor':(171,172)},
    ]   

    prepared_microdados = prepare_microdados('static/domicilios.txt', layout)

    click.echo('Inserindo dados de domicílios...') 

    insert_microdados_bundle_db(prepared_microdados, 'domicilios')

    return 

def insert_pessoas():
    layout = [{'uf':(0,2)}, {'municipio':(2,7)}, {'area_ponderacao':(7,20)}, {'controle':(20,28)}, {'peso':(28,44)}, {'regiao':(44,45)}, {'mesoregiao':(45,47)}, 
        {'microregiao':(47,50)}, {'regiao_metropolitana':(50,52)}, {'situacao_domicilio':(52,53)}, {'relacao_responsavel':(53,55)}, {'ordem_logica':(55,57)},
        {'sexo':(57,58)}, {'auxiliar_idade_anos_meses':(58,61)}, {'auxiliar_idade_anos':(61,64)}, {'auxiliar_idade_meses':(64,66)}, {'forma_declaracao_idade':(66,67)},
        {'cor_raca':(67,68)}, {'registro_nascimento':(68,69)}, {'dificuldade_enxergar':(69,70)}, {'dificuldade_ouvir':(70,71)}, {'dificuldade_caminhar':(71,72)},
        {'dificuldade_intelectual':(72,73)}, {'nasceu_no_municipio':(73,74)}, {'nasceu_na_uf':(74,75)}, {'nacionalidade':(75,76)}, {'ano_fixou_residencia':(76,80)},
        {'uf_ou_pais_nascimento':(80,81)}, {'uf_nascimento':(81,88)}, {'pais_nascimento':(88,95)}, {'tempo_moradia_uf':(95,98)}, {'tempo_moradia_municipio':(98,101)},
        {'uf_ou_pais_anterior':(101,102)}, {'uf_anterior':(102,109)}, {'municipio_anterior':(109,116)}, {'pais_anterior':(116,123)}, {'le_escreve':(145,146)},
        {'frequenta_escola':(146,147)}, {'curso_que_frequenta':(147,149)}, {'serie_ano':(149,151)}, {'serie':(151,152)}, {'conclusao_outra_graduacao':(152,153)},
        {'curso_mais_elevado':(153,155)}, {'conclusao_curso_mais_elevado':(155,156)}, {'especie_curso_mais_elevado':(156,157)}, {'nivel_instrucao':(157,158)},
        {'curso_graduacao':(158,161)}, {'curso_mestrado':(161,164)}, {'curso_doutorado':(164,167)}, {'conjuge':(189,190)}, {'natureza_uniao_conjuge':(192,193)},
        {'estado_civil':(193,194)}, {'quantidade_trabalhos':(198,199)}, {'relacao_trabalho':(208,209)}, {'':(198,199)}, {'contribuinte':(210,211)}, {'tipo_rendimento':(211,212)},
        {'valor_rendimento_principal':(212,218)}, {'bolsa_familia':(318,319)}, {'outros_programas':(319,320)}, {'possui_filhos':(351,352)},  {'quantidade_filhos':(356,358)}, 
        {'quantidade_filhos_vivos':(363,365)}, {'economicamente_ativa':(390,391)},
    ]   

    prepared_microdados = prepare_microdados('static/pessoas.txt', layout)

    click.echo('Inserindo dados de pessoas...')
    
    insert_microdados_bundle_db(prepared_microdados, 'pessoas')    

    return

def insert_mortalidade():
    layout = [{'uf':(0,2)}, {'municipio':(2,7)}, {'area_ponderacao':(7,20)}, {'controle':(20,28)}, {'peso':(28,44)}, {'regiao':(44,45)}, {'mesoregiao':(45,47)}, 
        {'microregiao':(47,50)}, {'regiao_metropolitana':(50,52)}, {'situacao_domicilio':(52,53)}, {'mes_ano_falecimento':(53,55)}, {'sexo':(55,56)},
         {'idade':(56,59)}, {'idade_meses':(59,61)},
    ]

    prepared_microdados = prepare_microdados('static/mortalidade.txt', layout)

    click.echo('Inserindo dados de mortalidade...')

    insert_microdados_bundle_db(prepared_microdados, 'mortalidade')    

    return

def prepare_microdados(resource, layout):
    click.echo('Preparando microdados em {}...'.format(resource))
    bundle_microdados = {}
    
    with app.open_resource(resource) as f:
        areas_ponderacao_index = '1'
        for line in f:
            area_ponderacao_microdado = line.decode()[slice(7, 20)]
            area_ponderacao_microdado_ref = area_ponderacao_microdado+'_'+areas_ponderacao_index
            if area_ponderacao_microdado_ref in [x for x in areas_ponderacao]:
                prepared_line = {list(slc.keys())[0]: line[slice(*slc[list(slc.keys())[0]])].decode() for slc in layout}
                if area_ponderacao_microdado_ref in bundle_microdados:
                    bundle_microdados[area_ponderacao_microdado_ref].append(prepared_line)
                else:
                    bundle_microdados[area_ponderacao_microdado_ref] = [prepared_line]
            areas_ponderacao_index = ( '2' if areas_ponderacao_index == '1' else '1') 

        return bundle_microdados

def insert_microdados_bundle_db(prepared_microdados, context):
    collection = mongo.db.areas_ponderacao

    for key in [x for x in areas_ponderacao]:
        if key in prepared_microdados:
            collection.update({
                "_id": key,     
                }, {
                    "$set":{'microdados.'+context : prepared_microdados[key]}
                }
            )
    return                 