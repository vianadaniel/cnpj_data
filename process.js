// Importando dependências
const config = require('./src/config');
const fs = require('fs').promises;
const path = require('path');
const { createDatabase, createAdditionalIndices } = require('./src/database/schema');
const { processEmpresasFile } = require('./src/processors/empresas');
const { processEstabelecimentosFile } = require('./src/processors/estabelecimentos');
const { processCnaeFile } = require('./src/processors/cnae');
const { processMotivosFile } = require('./src/processors/motivos');
const { processMunicipiosFile } = require('./src/processors/municipios');
const { processNaturezasFile } = require('./src/processors/naturezas');
const { processPaisesFile } = require('./src/processors/paises');
const { processQualificacoesFile } = require('./src/processors/qualificacoes');
const { processSimplesFile } = require('./src/processors/simples');
const { processSociosFile } = require('./src/processors/socios');

// Função principal para processamento
async function processExtractedFiles() {
    try {
        console.log('Iniciando processamento de dados CNPJ para PostgreSQL');

        // Ler informações sobre arquivos extraídos
        const extractInfoDir = path.join(__dirname, 'extracted_files_info');
        const infoFilePath = path.join(extractInfoDir, 'extracted_files.json');

        let extractedFilesInfo;
        try {
            const fileContent = await fs.readFile(infoFilePath, 'utf8');
            extractedFilesInfo = JSON.parse(fileContent);
        } catch (error) {
            console.error('Erro ao ler informações de arquivos extraídos:', error);
            console.error('Execute primeiro o script de download e extração (download.js)');
            process.exit(1);
        }

        // Criar banco de dados e índices
        await createDatabase();
        await createAdditionalIndices();

        // Processar arquivos de referência primeiro
        console.log('Processando arquivos de referência...');
        if (extractedFilesInfo.Cnaes?.[0]) await processCnaeFile(extractedFilesInfo.Cnaes[0], true);
        if (extractedFilesInfo.Motivos?.[0]) await processMotivosFile(extractedFilesInfo.Motivos[0], true);
        if (extractedFilesInfo.Municipios?.[0]) await processMunicipiosFile(extractedFilesInfo.Municipios[0], true);
        if (extractedFilesInfo.Naturezas?.[0]) await processNaturezasFile(extractedFilesInfo.Naturezas[0], true);
        if (extractedFilesInfo.Paises?.[0]) await processPaisesFile(extractedFilesInfo.Paises[0], true);
        if (extractedFilesInfo.Qualificacoes?.[0]) await processQualificacoesFile(extractedFilesInfo.Qualificacoes[0], true);

        // Processar arquivos principais
        console.log('Processando arquivos principais...');

        // Processar empresas primeiro
        console.log('Processando arquivos de empresas...');
        for (let i = 0; i <= 9; i++) {
            const fileKey = `Empresas${i}`;
            if (extractedFilesInfo[fileKey]?.[0]) {
                console.log(`Processando arquivo de empresas ${i}...`);
                await processEmpresasFile(extractedFilesInfo[fileKey][0]);
            }
        }

        // Processar estabelecimentos depois
        console.log('Processando arquivos de estabelecimentos...');
        for (let i = 0; i <= 9; i++) {
            const fileKey = `Estabelecimentos${i}`;
            if (extractedFilesInfo[fileKey]?.[0]) {
                console.log(`Processando arquivo de estabelecimentos ${i}...`);
                await processEstabelecimentosFile(extractedFilesInfo[fileKey][0]);
            }
        }

        // Processar Simples Nacional
        if (extractedFilesInfo.Simples?.[0]) {
            await processSimplesFile(extractedFilesInfo.Simples[0], true);
        }

        // Processar sócios
        for (let i = 0; i <= 9; i++) {
            const fileKey = `Socios${i}`;
            if (extractedFilesInfo[fileKey]?.[0]) {
                await processSociosFile(extractedFilesInfo[fileKey][0], true);
            }
        }

        console.log('Processamento concluído com sucesso!');
    } catch (error) {
        console.error('Erro durante o processamento:', error);
        process.exit(1);
    }
}

// Executar processamento
processExtractedFiles();