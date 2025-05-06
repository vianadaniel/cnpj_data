// Importando dependências
const config = require('./src/config');
const { downloadFile, unzipFile } = require('./src/utils/file');
const { createDatabase, createAdditionalIndices } = require('./src/database/schema');
const { processEmpresasFile } = require('./src/processors/empresas');
const { processEstabelecimentosFile } = require('./src/processors/estabelecimentos');

// Função principal
async function main() {
    try {
        console.log('Iniciando aplicação de importação de dados CNPJ para SQLite');

        // Criar banco de dados
        const db = await createDatabase();

        // Processar cada arquivo configurado
        for (const fileName of config.filesToDownload) {
            // Baixar arquivo
            const zipPath = await downloadFile(fileName);

            // Descompactar arquivo e obter lista de arquivos extraídos
            const extractedFiles = await unzipFile(zipPath);

            if (extractedFiles.length === 0) {
                console.error(`Nenhum arquivo foi extraído de ${zipPath}`);
                continue;
            }

            // Usar o primeiro arquivo extraído
            const finalFilePath = extractedFiles[0];
            console.log(`Usando arquivo extraído: ${finalFilePath}`);

            // Processar o arquivo conforme seu tipo
            if (fileName.startsWith('Empresas')) {
                await processEmpresasFile(db, finalFilePath);
            } else if (fileName.startsWith('Estabelecimentos')) {
                await processEstabelecimentosFile(db, finalFilePath);
            }
        }

        // Criar índices adicionais para melhorar performance
        await createAdditionalIndices(db);

        console.log('Fechando conexão com o banco de dados');
        await db.close();

        console.log('Processo concluído com sucesso!');
        console.log(`Banco de dados disponível em: ${config.dbPath}`);

    } catch (error) {
        console.error('Erro durante o processamento:', error);
        process.exit(1);
    }
}

const json = {
    "Cnaes": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$Z.D50419.CNAECSV"
    ],
    "Empresas0": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y0.D50419.EMPRECSV"
    ],
    "Empresas1": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y1.D50419.EMPRECSV"
    ],
    "Empresas2": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y2.D50419.EMPRECSV"
    ],
    "Empresas3": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y3.D50419.EMPRECSV"
    ],
    "Empresas4": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y4.D50419.EMPRECSV"
    ],
    "Empresas5": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y5.D50419.EMPRECSV"
    ],
    "Empresas6": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y6.D50419.EMPRECSV"
    ],
    "Empresas7": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y7.D50419.EMPRECSV"
    ],
    "Empresas8": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y8.D50419.EMPRECSV"
    ],
    "Empresas9": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y9.D50419.EMPRECSV"
    ],
    "Estabelecimentos0": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y0.D50419.ESTABELE"
    ],
    "Estabelecimentos1": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y1.D50419.ESTABELE"
    ],
    "Estabelecimentos2": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y2.D50419.ESTABELE"
    ],
    "Estabelecimentos3": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y3.D50419.ESTABELE"
    ],
    "Estabelecimentos4": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y4.D50419.ESTABELE"
    ],
    "Estabelecimentos5": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y5.D50419.ESTABELE"
    ],
    "Estabelecimentos6": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y6.D50419.ESTABELE"
    ],
    "Estabelecimentos7": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y7.D50419.ESTABELE"
    ],
    "Estabelecimentos8": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y8.D50419.ESTABELE"
    ],
    "Estabelecimentos9": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y9.D50419.ESTABELE"
    ],
    "Motivos": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$Z.D50419.MOTICSV"
    ],
    "Municipios": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$Z.D50419.MUNICCSV"
    ],
    "Naturezas": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$Z.D50419.NATJUCSV"
    ],
    "Paises": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$Z.D50419.PAISCSV"
    ],
    "Qualificacoes": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$Z.D50419.QUALSCSV"
    ],
    "Simples": [
        "/home/viana/Documents/cnpj_data/dados/F.K03200$W.SIMPLES.CSV.D50419"
    ],
    "Socios0": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y0.D50419.SOCIOCSV"
    ],
    "Socios1": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y1.D50419.SOCIOCSV"
    ],
    "Socios2": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y2.D50419.SOCIOCSV"
    ],
    "Socios3": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y3.D50419.SOCIOCSV"
    ],
    "Socios4": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y4.D50419.SOCIOCSV"
    ],
    "Socios5": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y5.D50419.SOCIOCSV"
    ],
    "Socios6": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y6.D50419.SOCIOCSV"
    ],
    "Socios7": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y7.D50419.SOCIOCSV"
    ],
    "Socios8": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y8.D50419.SOCIOCSV"
    ],
    "Socios9": [
        "/home/viana/Documents/cnpj_data/dados/K3241.K03200Y9.D50419.SOCIOCSV"
    ]
}

// Executar a aplicação
main();