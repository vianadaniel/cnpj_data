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
        console.log('Iniciando processamento de dados CNPJ para SQLite');

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

        // Criar banco de dados
        const db = await createDatabase();

        // Processar cada arquivo extraído
        for (const [fileName, extractedFiles] of Object.entries(extractedFilesInfo)) {
            if (extractedFiles.length === 0) {
                console.warn(`Nenhum arquivo extraído encontrado para ${fileName}`);
                continue;
            }

            // Usar o primeiro arquivo extraído
            const finalFilePath = extractedFiles[0];
            console.log(`Processando arquivo: ${finalFilePath}`);

            // Processar o arquivo conforme seu tipo
            if (fileName.startsWith('Empresas')) {
                await processEmpresasFile(db, finalFilePath);
            } else if (fileName.startsWith('Estabelecimentos')) {
                await processEstabelecimentosFile(db, finalFilePath);
            } else if (fileName.startsWith('CNAE')) {
                await processCnaeFile(db, finalFilePath);
            } else if (fileName.startsWith('Motivos')) {
                await processMotivosFile(db, finalFilePath);
            } else if (fileName.startsWith('Municipios')) {
                await processMunicipiosFile(db, finalFilePath);
            } else if (fileName.startsWith('Naturezas')) {
                await processNaturezasFile(db, finalFilePath);
            } else if (fileName.startsWith('Paises')) {
                await processPaisesFile(db, finalFilePath);
            } else if (fileName.startsWith('Qualificacoes')) {
                await processQualificacoesFile(db, finalFilePath);
            } else if (fileName.startsWith('Simples')) {
                await processSimplesFile(db, finalFilePath);
            } else if (fileName.startsWith('Socios')) {
                await processSociosFile(db, finalFilePath);
            }
        }

        // Criar índices adicionais para melhorar performance
        await createAdditionalIndices(db);

        console.log('Fechando conexão com o banco de dados');
        await db.close();

        console.log('Processamento concluído com sucesso!');
        console.log(`Banco de dados disponível em: ${config.dbPath}`);

    } catch (error) {
        console.error('Erro durante o processamento:', error);
        process.exit(1);
    }
}

// Executar o processamento
processExtractedFiles();