'''
Feito por:
    Alexandre Follador Guedes
    Marcello Henrique Martins
    
TF-IDF sem auxilio de nltk
'''

import math

def tf(document):
    lista = document.split(' ')
    wordDict = dict.fromkeys(lista, 0)
    tamanho = len(lista)

    for word in lista:
        wordDict[word]+=1

    for word in lista:
        wordDict[word] = wordDict[word] / tamanho
    return wordDict

def idf (corpus, termo):
    N = len(corpus)
    i = 0

    for sentence in corpus:
        if any(termo in s for s in sentence.split(' ')):
            i += 1

    return math.log(N/i, 10)

def tf_idf(termo, documento):
    global corpus
    term_frequency = tf(documento)
    invdf = idf(corpus, termo)
    try:
        return term_frequency[termo] * invdf
    except:
        return 0

d1 = 'O rato roeu a roupa do rei de Roma'
d2 = 'Nenhum rato rói a roupa do rei de Roma sem punição'
d3 = 'A rota de fuga do rato foi rápida'

corpus = [d1, d2, d3]

print(tf_idf("O",d1))
print(tf_idf("roeu",d1))
print(tf_idf("roupa",d2))
print(tf_idf("Roma",d3))
'''
tf-idf("O",d1) = 0.05301347274662915
tf-idf("roeu",d1) = 0.05301347274662915
tf-idf("roupa",d2) = 0.016008296277789203
tf-idf("Roma",d3) = 0.0
'''
