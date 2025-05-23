const path = require('path');

module.exports = {
    downloadDir: path.join(__dirname, '..', 'dados'),
    dbUser: process.env.DB_USER || 'cnpj',
    dbHost: process.env.DB_HOST || '3.91.197.56',
    dbName: process.env.DB_NAME || 'cnpjdb',
    dbPassword: process.env.DB_PASSWORD || 'cnpj123',
    dbPort: process.env.DB_PORT || 5432,
    baseUrl: 'https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/2025-04-19',
    // Lista completa de arquivos a serem baixados
    filesToDownload: [
        'Cnaes',
        'Empresas0', 'Empresas1', 'Empresas2', 'Empresas3', 'Empresas4',
        'Empresas5', 'Empresas6', 'Empresas7', 'Empresas8', 'Empresas9',
        'Estabelecimentos0', 'Estabelecimentos1', 'Estabelecimentos2', 'Estabelecimentos3',
        'Estabelecimentos4', 'Estabelecimentos5', 'Estabelecimentos6', 'Estabelecimentos7',
        'Estabelecimentos8', 'Estabelecimentos9',
        'Motivos',
        'Municipios',
        'Naturezas',
        'Paises',
        'Qualificacoes',
        'Simples',
        'Socios0', 'Socios1', 'Socios2', 'Socios3', 'Socios4',
        'Socios5', 'Socios6', 'Socios7', 'Socios8', 'Socios9'
    ]
};
