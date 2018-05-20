import re
from flask import render_template
from flask import jsonify

from aglomerados_subnormais import app, mongo
from flask_pymongo import PyMongo

@app.route('/')
def index():
    app.logger.warning('sample message')
    return render_template('index.html')


@app.route('/<geocodigo>/pessoas_por_sexo/')
def pessoas_por_sexo(geocodigo):
    areas_db = mongo.db.areas_ponderacao
    total = 0
    masc = 0
    fem = 0
    percentual_masc = 0
    percentual_fem = 0
    
    for bundle_amostra in areas_db.find(
        {'geocodigo': geocodigo},{'microdados.pessoas.sexo':'1'}
    ):
      
        for x in bundle_amostra['microdados']['pessoas']:
            if x['sexo'] == '2':
                fem += 1
                total += 1
            elif x['sexo'] == '1':
                masc += 1 
                total += 1

    if masc and total:
        percentual_masc = round(masc / total * 100, 2)
        
    if fem and total:        
        percentual_fem = round(fem / total * 100, 2)

    return jsonify({'result': {'masculino':{'total': masc, 'percentual': percentual_masc},
                            'feminino': {'total': fem, 'percentual': percentual_fem},
                            'total': total}
                    })

@app.route('/<geocodigo>/nacionalidade/')
def pessoas_por_nacionalidade(geocodigo):
    areas_db = mongo.db.areas_ponderacao
    total = 0
    brasileiro = 0
    estrangeiro = 0
    percentual_bras = 0
    percentual_estr = 0

    for bundle_amostra in areas_db.find(
        {'geocodigo': geocodigo},{'microdados.pessoas.nacionalidade':'1'}
    ):
        for x in bundle_amostra['microdados']['pessoas']:
            if x['nacionalidade'] == '1':
                brasileiro += 1
                total += 1
            elif x['nacionalidade'] in ('2', '3'):
                estrangeiro += 1 
                total += 1
 
    if brasileiro and total:
        percentual_bras = round(brasileiro / total * 100, 2)
    if estrangeiro and total:
        percentual_estr = round(estrangeiro / total * 100, 2)

    return jsonify({'result': {'brasileiro':{'total': brasileiro, 'percentual': percentual_bras},
                            'estrangeiro': {'total': estrangeiro, 'percentual': percentual_estr},
                            'total': total}
                    })


@app.route('/<geocodigo>/populacao/faixa-etaria/')
def populacao_por_idade(geocodigo):
    areas_db = mongo.db.areas_ponderacao

    faixas = {'0_3':(0, 3),'4_5':(4, 5),'6':(6, 6),'7_14':(7,14),'15_17':(15,17),'18_19':(18,19),'20_24':(20,24),'25':(25,140)}
    total_faixas = {key:0 for key in faixas}
    total = 0

    for bundle_amostra in areas_db.find(
        {'geocodigo': geocodigo},{'microdados.pessoas.auxiliar_idade_anos':'1','microdados.pessoas.auxiliar_idade_meses':'1'}
    ):
        for x in bundle_amostra['microdados']['pessoas']:
            if x['auxiliar_idade_anos']:
                for key, faixa in faixas.items():
                    if faixa[0] <= int(x['auxiliar_idade_anos']) and faixa[1] >= int(x['auxiliar_idade_anos']):
                        total_faixas[key] +=1
                total+=1
            elif x['auxiliar_idade_meses']:
                total_faixas['0_3'] +=1
                total+=1

    return jsonify({'result': {'faixas': total_faixas,
                            'total': total}
                    })


@app.route('/<geocodigo>/populacao/deficiencia/')
def populacao_com_deficiencia(geocodigo):
    areas_db = mongo.db.areas_ponderacao

    pessoas_com_deficiencia = {
        'dificuldade_enxergar':{
            'label': 'Dificuldade para enxergar',
            'nao_possui': 0,
            'leve': 0,
            'moderada': 0,
            'alta': 0,
            },
        'dificuldade_ouvir':{
            'label': 'Dificuldade para ouvir',
            'nao_possui': 0,
            'leve': 0,
            'moderada': 0,
            'alta': 0,
            },
        'dificuldade_caminhar':{
            'label': 'Dificuldade para caminhar',
            'nao_possui': 0,
            'leve': 0,
            'moderada': 0,
            'alta': 0,
            },
        'dificuldade_intelectual':{
            'label': 'Dificuldade intelectual',
            'nao_possui': 0,
            'leve': 0,
            'moderada': 0,
            'alta': 0,
            },
        'total':0,
    }

    tipos_deficiencia = ['dificuldade_enxergar','dificuldade_ouvir','dificuldade_caminhar','dificuldade_intelectual']
    grau_deficiencia_map = {'4': 'nao_possui', '3':'leve','2': 'moderada','1':'alta'}

    for bundle_amostra in areas_db.find(
        {'geocodigo': geocodigo},{'microdados.pessoas.dificuldade_enxergar':'1',
            'microdados.pessoas.dificuldade_ouvir':'1','microdados.pessoas.dificuldade_caminhar':'1',
            'microdados.pessoas.dificuldade_intelectual':'1'}
    ):

        for x in bundle_amostra['microdados']['pessoas']:
            contabilizada = False
            for deficiencia in tipos_deficiencia:

                if x[deficiencia]:
                    if x[deficiencia] in grau_deficiencia_map:
                        if not contabilizada:
                            pessoas_com_deficiencia['total'] +=1 
                        pessoas_com_deficiencia[deficiencia] [grau_deficiencia_map[ x[deficiencia] ] ] += 1
                        contabilizada = True

    return jsonify({'result': {'pessoas_com_deficiencia': pessoas_com_deficiencia}})
