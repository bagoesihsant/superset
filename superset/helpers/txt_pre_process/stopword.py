# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

def lowercase_text(text):
    return str.lower(text)

def get_stopwords():
    # Inisialisasi stopword remover factory
    factory = StopWordRemoverFactory()
    stopwords = factory.get_stop_words()
    return stopwords

def init_stemmer():
    stem_factory = StemmerFactory()
    stemmer = stem_factory.create_stemmer()
    return stemmer

def remove_stopword(text):
    # Membuat kumpulan stopwords
    stopwords = get_stopwords()

    # Menghapus Stopwords
    cleaned_word = [word for word in text.split() if word not in stopwords]
    cleaned_word = ' '.join(cleaned_word)
    return cleaned_word

def stemming_word(text):
    # Memanggil stemmer
    stemmer = init_stemmer()

    # Proses Stemming
    stem_word = [stemmer.stem(word) for word in text.split()]
    stem_word = ' '.join(stem_word)
    return stem_word