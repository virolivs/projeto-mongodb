// CONSULTAS À COLEÇÃO SONGS

// quantas músicas são da cantora taylor swift
db.songs.find({"artist.name": "Taylor Swift"}).count() // quando for um "atributo especifico vc tem que usar aspas"]

// contar quantas músicas da taylor swift ou da lana del rey possuem o jack antonoff como produtor
db.songs.find({
    $and: [
        {
            $or: [
                { "artist.name": "Taylor Swift" },
                { "artist.name": "Lana Del Rey" }
            ]
        },
        { "credits.producers.name": { $in: ["Jack Antonoff"] } }
    ]
}).count();


// contar musicas a partir de 2000 que possuam mais de 3 minutos 
db.songs.find({duration: {$gt: 180}, releaseDate: {$gte:"2000-01-01"}}).count() // colocar os dentro do mesmo par de chaves

// musicas da taylor lancadas a apartir de 
db.songs.find({$and:[{ "releaseDate": { $gt: "2020-01-01" } }, {"artist.name": "Taylor Swift"}]}).pretty();


// o artista com a maior média de duração de musicas
db.songs.aggregate([
    {
        $group: {
            _id: "$artist.name", 
            duracao: { $avg: "$duration" }
        }
    },
    {
        $sort: { "duracao": -1 }
    },
    {
        $limit: 1
    }
])

// match: filtrar algo em casos de agregação
// a duração média em minutos de músicas que possuem mais do que 800000 plays

db.songs.aggregate([
    {
        $match: { plays: { $gt: 500000000 } }
    },
    {
        $group: {
            _id: null,
            mediaMusicas: { $avg: "$duration" } // Calcula a média em segundos
        }
    },
    {
        $project: {
            _id: 0,
            mediaMusicasMinutos: { $divide: ["$mediaMusicas", 60] } // Divide por 60 para converter para minutos
        }
    }
]);


// como usar $where: -- funcoes javascript com this.o atributo
//  encontra músicas de artistas populares 
//(definidos aqui como artistas com mais de 1 milhão de reproduções em suas músicas) e lançadas em 2022 ou depois
db.songs.find({
    $where: "this.plays > 1000000 && new Date(this.releaseDate) >= new Date('2022-01-01')"
});

// generos cujas musicas possuem em media tempo de duracao maior ou igual a 6 minutos (360 segundos)
db.songs.aggregate([
    // splita o array de generos 
    {
        $unwind: "$genres"
    },
    {
        $group: {
            _id: "$genres", 
            mediaDuracao: { $avg: "$duration" }
        }
    },
    {
        $match: {
            mediaDuracao: { $gte: 360 }
        }
    }
]);

// uso do cond e do project
// project: é utilizado dentro do pipeline aggregate e é responsável por definir quais campos irão
// aparecer na consulta
// nesse caso, queremos apenas os que determinam o cond, então exibimos apenas eles
// também podemos criar um novo campo, como o classification abaixo: 
// ele é setado conforme o cond
db.songs.aggregate([
    { 
        $match: { "artist.name": "Olivia Rodrigo" } // Filtra as músicas de Taylor Swift
    },
    { 
        $project: {
            title: 1,
            duration: 1,
            classification: {
                $cond: {
                    if: { $gte: ["$duration", 200] }, // Se a duração for maior que 200 seg
                    then: "Não tão curta",
                    else: "Curta"
                }
            }
        }
    }
]);

// deixar essa pra deipois
db.songs.aggregate([
    {
        $project: {
            title: 1,
            releaseDate: 1,
            newOrOld: {
                $cond: {
                    if: {
                        $expr: { $gt: ["$releaseDate", "2020-01-01"] }
                    },
                    then: "Música atual",
                    else: "Música antiga"
                }
            }
        }
    }
]);

// usando o sum para contar quantas músicas existem na coleção e agrupá-las por gênero
db.songs.aggregate([
    {
        $unwind: "$genres"
    },
    {
        $group:{
            _id: "$genres",
            count:{$sum: 1} //adiciona 1 para cada instancia desse objeto
        }
    }
])

// mais aggregate
// total de plays por artista

db.songs.aggregate([
    {
        $group: {
            _id: "$artist",          
            totalPlays: { $sum: "$plays" }  
        }
    }
])

//total de plays cujo gênero é rock progressivo OU possui duração acima de 360seg (6 min)
db.songs.aggregate([
    {
        $match: {
            $or: [
                { genres: { $in: ["Progressive rock"] } }, 
                { duration: { $gt: 360 } }                
            ]
        }
    },
    {
        $group: {
            _id: null,                      
            totalPlays: { $sum: "$plays" }  
        }
    }
]);

// duração máxima das musicas por artista, exibindo suas informaçãoes com o project
db.songs.aggregate([
    {
      $group: {
        _id: "$artist.name",
        maxDuration: { $max: "$duration" },
        songTitle: "$title" ,
        albumTitle: "$album.title" ,
        releaseDate: "$releaseDate" ,
        plays: { $first: "$plays" }
      }
    },
    {
      $sort: { maxDuration: -1 }
    },
    {
      $project: {
        _id: 0,
        artist: "$_id",
        maxDuration: 1,
        songTitle: 1,
        albumTitle: 1,
        releaseDate: 1,
        plays: 1
      }
    }
  ]);
  
// musicas da taylor que possuem mais de um compositor

db.songs.aggregate([
    {
        $match: {
            "artist.name": "Taylor Swift",
            "credits.composers": { $exists: true, $not: { $size: 0 } }
        }
    },
    {
        $project: {
            _id: 0,
            "credits.composers": 1 
        }
    }
])

// musicas com "play"
db.songs.createIndex({ // cria index text
    lyrics: "text",
    title: "text",
    "album.title": "text"
})


db.songs.aggregate([
    {
        $match: { $text: { $search: "love" } } // procura pela palavra "love"
    },
    {
        $project: {
            _id: 0,
            title: 1,                
            "artist.name": 1,         
            "album.title": 1,         
            lyrics: 1,                
            score: { $meta: "textScore" } // pontuacao de relevancia
        }
    },
    {
        $sort: { score: { $meta: "textScore" } } 
    }
])

// filtrar músicas de gênero rock nacional
db.songs.aggregate([
    {
        $project: {
            title: 1,
            isRockNational: {
                $in: ["rock nacional", "$genres"]
            },
            genres: {
                $filter: {
                    input: "$genres",
                    as: "genre",
                    cond: { $eq: ["$$genre", "rock nacional"] }
                }
            }
        }
    },
    {
        $match: {
            isRockNational: true
        }
    }
])


// converte a duração das músicas de segundos para minutos
db.songs.aggregate([
    {
        $set: {
            durationInMinutes: { $divide: ["$duration", 60] } 
        }
    },
    {
        $project: {
            title: 1,
            duration: 1,
            durationInMinutes: 1
        }
    }
])

// músicas que são pop e folk ao mesmo tempo
db.songs.aggregate([
    {
        $match: {
            genres: {
                $all: ["pop", "folk"] 
            }
        }
    },
    {
        $project: {
            title: 1,
            genres: 1
        }
    }
])

// retorna o usuario que mais escuta musica 
db.usuario.aggregate([
    {
        $project:{
            name:1,
            quantidade_musica:{$size:"$likedSongs"}
        }
    },
    {
        $sort: {quantidade_musica: -1}
    },
    {
        $limit: 1
    }
]);
//Função que calcula a idade e adiciona o campo idade
function calcularIdade(dataNascimento) {
    const hoje = new Date();
    const nascimento = new Date(dataNascimento);
    let idade = hoje.getFullYear() - nascimento.getFullYear();
    const mes = hoje.getMonth() - nascimento.getMonth();

    if (mes < 0 || (mes === 0 && hoje.getDate() < nascimento.getDate())) {
        idade--;
    }
    return idade;
}

db.usuario.find().forEach(function(usuario) {
    const idade = calcularIdade(usuario.birthDate);
    db.usuario.updateOne(
        {_id:usuario._id},
        {$set:{idade:idade}}
    );
    print("Nome: " + usuario.name + ", Idade: " + idade);
});
//retorna o usuario mais velhor
db.usuario.aggregate([
    {
        $group:{
            _id:null,
            maxValor:{$max: "$idade"}
        }
    }
]);
// garantir que todos os usuarios possuem idade
db.usuario.countDocuments({ "idade": { "$exists": true } })

//renomeando a coleção usuarios para clientes
db.adminCommand({
    renameCollection: "spotify.usuario", 
    to: "spotify.clientes"
});
// retorna usuario que é do estado de sao paulo e escuta mais de 3 musicas
db.clientes.findOne({
    "state": "São Paulo",               // Condição: estado igual a "São Paulo"
    "likedSongs.3": { $exists: true }   // Condição: mais de 3 músicas na lista
});

// adicionar musicas á liked songs de um determinado usuario 
db.clientes.updateOne(
  { "name": "Igor Lima" }, // Filtro para encontrar Julia Ferreira
  {
    $addToSet: {
      likedSongs: {
        $each: [
          { "$oid": "60d21b4667d0d8992e610cde" }, // ID da música "Stay"
          { "$oid": "60d21b4667d0d8992e610ce1" }  // ID da música "Peaches"
        ]
      }
    }
  }
);
// retorna as informações das musicas escutadas por um determinado usuario
  db.clientes.aggregate([
    {
        $match: {
            name: "Lucas Oliveira"  
        }
    },
    {
        $lookup: {
            from: "songs",                    
            localField: "likedSongs.oid",       
            foreignField: "_id.oid",            
            as: "musicasDetalhadas"               
        }
    },
    {
        $project: {
            name: 1,                               
            musicasDetalhadas: 1                    
        }
    }
]);
db.clientes.aggregate([
    {
        $match: {
            name: "Lucas Oliveira"  
        }
    },
    {
        $lookup: {
            from: "songs",                    
            localField: "likedSongs.oid",       
            foreignField: "_id.oid",            
            as: "musicasDetalhadas"               
        }
    },
    {
        $addFields: {
            totalMusicas: { $size: "$musicasDetalhadas" }  // Conta o número de músicas detalhadas
        }
    },
    {
        $project: {
            name: 1,                              
            musicasDetalhadas: 1,                 
            totalMusicas: 1                       // Exibe a contagem das músicas
        }
    }
]);

db.clientes.aggregate([
    {
        $match: {
            name: "Lucas Oliveira"  
        }
    },
    {
        $lookup: {
            from: "songs",                    
            localField: "likedSongs.oid",       // Campo contendo a lista de músicas curtidas
            foreignField: "_id.oid",            // Comparação direta com o _id da coleção "musicas"
            as: "musicasDetalhadas"         // As músicas detalhadas são armazenadas aqui
        }
    },
    {
        $project: {
            name: 1,                              
            musicasDetalhadas: {
                $filter: {                        // Aplica um filtro para garantir que só as músicas curtidas sejam mostradas
                    input: "$musicasDetalhadas",
                    as: "musica",
                    cond: { 
                        $in: ["$$musica._id", "$likedSongs"]  // Verifica se o _id da música está na lista de likedSongs
                    }
                }
            }
        }
    },
    {
        $addFields: {
            totalMusicas: { $size: "$musicasDetalhadas" }  // Conta o número de músicas detalhadas
        }
    },
    {
        $project: {
            name: 1,                              
            musicasDetalhadas: 1,                 
            totalMusicas: 1                       // Exibe a contagem das músicas
        }
    }
]);


  